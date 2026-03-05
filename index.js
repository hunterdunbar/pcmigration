require('dotenv').config();

const  { 
    getTableMetadata,
    query,
    queryCursor,
} = require('./services/db');

const { JOB_STATUS } = require('./services/utils');
const {
    COMPRESSED_COLUMN_NAME,
    shouldCompressTable,
    shouldCompressFieldByLength,
    maybeCompressFieldValue
} = require('./services/migrationCompression');



const { getExternalIdFieldName, getMappedFieldName } = require('./services/salesforce')
const { processInfoLogging } = require('./services/processInfo');

const { 
    sourceTable,
    targetTable,
    hcSchema,
    pcSchema,
    clientDbUrl,
    numberOfThreads,
    bulkLimit
} = require('./config/default')

const cluster = require('cluster');
// Used in worker compressed path (rowsPerInsertChunk) to stay under Postgres bind parameter limit.
const MAX_QUERY_PARAMS = 60000;
// Used in worker compressed path (cursorChunkSize) to cap rows fetched per cursor read.
const MAX_GZIP_CURSOR_CHUNK_ROWS = 1000;
// Used by master maybeLogProcessInfo() to throttle processInfo logging frequency.
const PROCESS_INFO_LOG_MIN_INTERVAL_MS = process.env.MIGRATED_STANDRD_OBJECT_PREFIX || 10000;
const PCMA_VIEW_NAME = 'pcma_tables_info_mv';

// Resolves a single boolean flag for the whole migration run:
// should workers use compressed path for emailmessage.htmlbody or not.
// Reads max htmlbody length from pcma_tables_info_mv once in master process.
async function resolveShouldCompressByMaxLength(tableName) {
    // Compression rule applies only to emailmessage.htmlbody.
    if (!shouldCompressTable(tableName)) {
        return false;
    }

    try {
        // Read max observed htmlbody length from the prebuilt materialized view once in master.
        const maxLengthRes = await query(
            `select column_size::bigint as max_length
             from ${pcSchema}.${PCMA_VIEW_NAME}
             where table_name = $1 and column_name = $2
             limit 1`,
            [String(tableName || '').toLowerCase(), COMPRESSED_COLUMN_NAME]
        );
        const maxLength = Number(maxLengthRes?.rows?.[0]?.max_length);
        return shouldCompressFieldByLength(tableName, COMPRESSED_COLUMN_NAME, maxLength);
    } catch (e) {
        // If materialized-view data is unavailable, keep compression enabled as a safe fallback.
        return true;
    }
}

(async () => {

    if (!clientDbUrl) {
        throw new Error('CLIENT_DATABASE_URL is not defined')
    }

    if (!sourceTable) {
        throw new Error('SOURCE_TABLE is not defined')
    }
    
    if (!targetTable) {
        throw new Error('TARGET_TABLE is not defined')
    }

    const processInfo = {
        sourceTable,
        targetTable
    }

    if (cluster.isMaster) {
        //need to get list of fields from source table
        const tableMetada = await getTableMetadata(pcSchema, sourceTable);
        const sourceColumnNames = tableMetada?.rows.map(r => `${r.columnName}`) || [];
        const targetColumnNames = tableMetada?.rows.map((r, i) => getMappedFieldName(r.columnName, i)) || [];
        const sourceColumns = sourceColumnNames.join(',');
        const targetColumns = targetColumnNames.join(',');
        // Compute once in master and pass the boolean to every worker job.
        const shouldCompress = await resolveShouldCompressByMaxLength(sourceTable);
        const bulkSize = Number(bulkLimit);

        if (!Number.isFinite(bulkSize) || bulkSize <= 0) {
            throw new Error(`BULK_LIMIT is invalid: ${bulkLimit}`);
        }

        //get count of objects
        const countOfRowsInTargetTableRes = await query(`select count(*) from ${hcSchema}.${targetTable.toLowerCase()}`);
        const countOfRowsInTargetTable = countOfRowsInTargetTableRes?.rows?.[0]?.count;
        const initialMigratedRecords = Number(countOfRowsInTargetTable) || 0;

        //get count of objects
        const countOfRowsRes = await query(`select count(*) from ${pcSchema}.${sourceTable.toLowerCase()}`);
        const countOfRows = countOfRowsRes?.rows?.[0]?.count;
        const idBoundsRes = await query(`select min(id) as min_id, max(id) as max_id from ${pcSchema}.${sourceTable.toLowerCase()}`);
        const minId = Number(idBoundsRes?.rows?.[0]?.min_id);
        const maxId = Number(idBoundsRes?.rows?.[0]?.max_id);

        const countOfJobs = Number.isFinite(minId) && Number.isFinite(maxId)
            ? Math.ceil((maxId - minId + 1) / bulkSize)
            : 0;
        const queue = [];

        //create list of queries
        for (let i = 0; i < countOfJobs; i++) {
            const idFrom = minId + (i * bulkSize);
            const idTo = Math.min(maxId, idFrom + bulkSize - 1);
            queue.push({
                index : i,
                status : null,
                sourceColumns,
                targetColumns,
                sourceColumnNames,
                targetColumnNames,
                shouldCompress,
                insertedCount : 0,
                idFrom,
                idTo
                // query : 'test'
            })
        }

        processInfo.limitPerJob = bulkSize;
        processInfo.countOfRecordsToMigrate = countOfRows;
        processInfo.countOfThreads = numberOfThreads;
        processInfo.countOfJobs = countOfJobs;
        processInfo.countOfMigratedRecords = initialMigratedRecords;


        if (queue.length === 0) {
            processInfoLogging(processInfo)
            return;
        }

        
        console.log(`Master process ${process.pid} is running`);
        const migrationStartedAt = Date.now();
        let isFinalProcessInfoLogged = false;
        let isQueueCompletionLogged = false;
        let jobMonitor = null;
        let lastProcessInfoLogAt = 0;
        let lastProcessInfoSnapshot = null;

        const refreshProcessInfo = () => {
            processInfo.countOfCompletedJobs = queue.filter(j => j.status === JOB_STATUS.Completed)?.length;
            processInfo.countOfJobsWithError = queue.filter(j => j.status === JOB_STATUS.Error)?.length;
            processInfo.countOfRemainingJobs = processInfo.countOfJobs - processInfo.countOfCompletedJobs - processInfo.countOfJobsWithError;
            const insertedByWorkers = queue.reduce((sum, job) => sum + (Number(job.insertedCount) || 0), 0);
            processInfo.countOfMigratedRecords = initialMigratedRecords + insertedByWorkers;
        };

        const getProcessInfoSnapshot = () => ({
            completed : processInfo.countOfCompletedJobs || 0,
            error : processInfo.countOfJobsWithError || 0,
            remaining : processInfo.countOfRemainingJobs || 0
        });

        const maybeLogProcessInfo = (force = false) => {
            refreshProcessInfo();
            const snapshot = getProcessInfoSnapshot();
            const now = Date.now();
            const isChanged = !lastProcessInfoSnapshot
                || snapshot.completed !== lastProcessInfoSnapshot.completed
                || snapshot.error !== lastProcessInfoSnapshot.error
                || snapshot.remaining !== lastProcessInfoSnapshot.remaining;
            const isIntervalElapsed = (now - lastProcessInfoLogAt) >= PROCESS_INFO_LOG_MIN_INTERVAL_MS;

            if (force || (isChanged && isIntervalElapsed)) {
                processInfoLogging(processInfo);
                lastProcessInfoLogAt = now;
                lastProcessInfoSnapshot = snapshot;
            }
        };

        const tryFinalizeProcessInfo = (trigger) => {
            refreshProcessInfo();
            if (!isFinalProcessInfoLogged && processInfo.countOfRemainingJobs === 0) {
                isFinalProcessInfoLogged = true;
                if (jobMonitor) {
                    clearInterval(jobMonitor);
                }
                maybeLogProcessInfo(true);
                const elapsedSeconds = ((Date.now() - migrationStartedAt) / 1000).toFixed(2);
                console.info(`[MASTER] Migration finished in ${elapsedSeconds}s (trigger: ${trigger})`);
            }
        };
      
        for (let i = 0; i < numberOfThreads; i++) {
            const job = queue.find(j => !j.status);
            if (job) {
                job.status = JOB_STATUS.Pending;

                cluster.fork({
                    JOB : JSON.stringify(job)
                });
            } else {
                console.log('No jobs in queue');
                break;
            }
        }

        cluster.on('message', (worker, message) => {
            const msg = JSON.parse(message);
            const job = queue[msg.index];
            if (job) {
                if (msg.status) {
                    job.status = msg.status;
                }
                if (msg.insertedCount !== undefined && msg.insertedCount !== null) {
                    const insertedCount = Number(msg.insertedCount);
                    if (Number.isFinite(insertedCount)) {
                        job.insertedCount = insertedCount;
                    }
                }
            }
            maybeLogProcessInfo(false);
            tryFinalizeProcessInfo('message');
        });

        cluster.on('exit', (worker, code) => {
            if (code === 1) {
                console.error('There is a critial error with worker ' + worker.process.pid)
            } else {
                const nextJob = queue.find(j => !j.status);
                if (nextJob) {
                    nextJob.status = JOB_STATUS.Pending;
                    cluster.fork({
                        JOB : JSON.stringify(nextJob)
                    });
                } else {
                    if (!isQueueCompletionLogged) {
                        isQueueCompletionLogged = true;
                        const jobsWithError = queue.filter(j => j.status === JOB_STATUS.Error);
                        const completedJobs = queue.filter(j => j.status === JOB_STATUS.Completed);

                        console.debug('All jobs has been processed: ')
                        console.debug('Jobs with Error: ' + jobsWithError.length);
                        console.debug('Complted Jobs: ' + completedJobs.length);
                    }
                }
            }
            tryFinalizeProcessInfo('exit');
        });

        jobMonitor = setInterval(() => {
            maybeLogProcessInfo(false);
            tryFinalizeProcessInfo('timer');
        }, 10000) //

    } else {
        const currentJob = JSON.parse(process.env.JOB);
        let insertedCount = 0;
        const msg = {
            index : currentJob.index,
            status : JOB_STATUS.Processing,
            insertedCount
        }

        process.send(JSON.stringify(msg));

        const externalId = getExternalIdFieldName();
        const queryString = `insert into ${hcSchema}.${targetTable.toLowerCase()}(${currentJob.targetColumns}) select ${currentJob.sourceColumns} from ${pcSchema}.${sourceTable} where id between ${currentJob.idFrom} and ${currentJob.idTo} ON CONFLICT (${externalId}) DO NOTHING`;

        let critialError = false;
        try {
            if (!currentJob.shouldCompress) {
                const queryResult = await query(queryString);
                insertedCount = Number(queryResult?.rowCount) || 0;
            } else {
                // Compressed migration path: stream rows, compress required fields, insert in safe-sized batches.
                const columnsPerRow = currentJob.targetColumnNames.length;
                if (!columnsPerRow) {
                    throw new Error('No target columns found for compressed migration path');
                }

                // Split inserts to keep total bind params under MAX_QUERY_PARAMS.
                const rowsPerInsertChunk = Math.max(1, Math.floor(MAX_QUERY_PARAMS / columnsPerRow));
                // Cursor fetch size is additionally capped to avoid large in-memory gzip batches.
                const cursorChunkSize = Math.max(1, Math.min(rowsPerInsertChunk, MAX_GZIP_CURSOR_CHUNK_ROWS));
                const sourceSelectQuery = `select ${currentJob.sourceColumns} from ${pcSchema}.${sourceTable} where id between ${currentJob.idFrom} and ${currentJob.idTo} order by id`;

                await queryCursor(sourceSelectQuery, [], { chunkSize : cursorChunkSize }, async (sourceRows) => {
                    if (!sourceRows?.length) {
                        return;
                    }

                    for (let rowIndex = 0; rowIndex < sourceRows.length; rowIndex += rowsPerInsertChunk) {
                        const rowChunk = sourceRows.slice(rowIndex, rowIndex + rowsPerInsertChunk);
                        // Preserve source column order and apply gzip+base64 only for configured compressed fields.
                        const values = rowChunk.flatMap(row =>
                            currentJob.sourceColumnNames.map(sourceColumnName => {
                                const value = row[sourceColumnName] !== undefined
                                    ? row[sourceColumnName]
                                    : row[String(sourceColumnName).toLowerCase()];
                                return maybeCompressFieldValue(sourceTable, sourceColumnName, value);
                            })
                        );

                        // Build positional placeholders for multi-row VALUES (...),(...).
                        const placeholders = rowChunk.map((_, chunkRowIndex) => {
                            const rowPlaceholders = currentJob.targetColumnNames.map((__, colIndex) =>
                                `$${chunkRowIndex * columnsPerRow + colIndex + 1}`
                            );
                            return `(${rowPlaceholders.join(',')})`;
                        }).join(',');

                        const insertQuery = `insert into ${hcSchema}.${targetTable.toLowerCase()}(${currentJob.targetColumns}) values ${placeholders} ON CONFLICT (${externalId}) DO NOTHING`;
                        const insertResult = await query(insertQuery, values);
                        insertedCount += Number(insertResult?.rowCount) || 0;
                    }
                });
            }
            msg.insertedCount = insertedCount;
            msg.status = JOB_STATUS.Completed;
        } catch (e) {
            console.error('ERROR: ' + process.pid, { e, currentJob, queryString });
            msg.insertedCount = insertedCount;
            msg.status = JOB_STATUS.Error;
            critialError = true;
        }

        process.send(JSON.stringify(msg));
        process.exit(critialError ? 1 : 0);
    }

})();
