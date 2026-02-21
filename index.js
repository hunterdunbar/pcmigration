require('dotenv').config();

const  { 
    getTableMetadata,
    query,
    queryCursor,
} = require('./services/db');

const { JOB_STATUS } = require('./services/utils');
const {
    shouldCompressTable,
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
const MAX_QUERY_PARAMS = 60000;
const MAX_GZIP_CURSOR_CHUNK_ROWS = 1000;
const GZIP_PROGRESS_LOG_EVERY_INSERT_CHUNKS = 20;
const GZIP_COMPRESSED_COLUMN = 'htmlbody';

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
        const shouldCompress = shouldCompressTable(sourceTable);
        const bulkSize = Number(bulkLimit);

        if (!Number.isFinite(bulkSize) || bulkSize <= 0) {
            throw new Error(`BULK_LIMIT is invalid: ${bulkLimit}`);
        }

        //get count of objects
        const countOfRowsInTargetTableRes = await query(`select count(*) from ${hcSchema}.${targetTable.toLowerCase()}`);
        const countOfRowsInTargetTable = countOfRowsInTargetTableRes?.rows?.[0]?.count;

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
                idFrom,
                idTo
                // query : 'test'
            })
        }

        processInfo.limitPerJob = bulkSize;
        processInfo.countOfRecordsToMigrate = countOfRows;
        processInfo.countOfThreads = numberOfThreads;
        processInfo.countOfJobs = countOfJobs;
        processInfo.countOfMigratedRecords = countOfRowsInTargetTable;


        if (queue.length === 0) {
            processInfoLogging(processInfo)
            return;
        }

        
        console.log(`Master process ${process.pid} is running`);
        const migrationStartedAt = Date.now();
        let isFinalProcessInfoLogged = false;
        let jobMonitor = null;

        const refreshProcessInfo = () => {
            processInfo.countOfCompletedJobs = queue.filter(j => j.status === JOB_STATUS.Completed)?.length;
            processInfo.countOfJobsWithError = queue.filter(j => j.status === JOB_STATUS.Error)?.length;
            processInfo.countOfRemainingJobs = processInfo.countOfJobs - processInfo.countOfCompletedJobs - processInfo.countOfJobsWithError;
        };

        const tryFinalizeProcessInfo = (trigger) => {
            refreshProcessInfo();
            if (!isFinalProcessInfoLogged && processInfo.countOfRemainingJobs === 0) {
                isFinalProcessInfoLogged = true;
                if (jobMonitor) {
                    clearInterval(jobMonitor);
                }
                processInfoLogging(processInfo);
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
            queue[msg.index].status = msg.status;
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
                    const jobsWithError = queue.filter(j => j.status === JOB_STATUS.Error);
                    const completedJobs = queue.filter(j => j.status === JOB_STATUS.Completed);
                    
                    console.debug('All jobs has been processed: ')
                    console.debug('Jobs with Error: ' + jobsWithError.length);
                    console.debug('Complted Jobs: ' + completedJobs.length);
                }
            }
            tryFinalizeProcessInfo('exit');
        });

        jobMonitor = setInterval(() => {
            refreshProcessInfo();
            processInfoLogging(processInfo);
            tryFinalizeProcessInfo('timer');
        }, 10000) //

    } else {
        const currentJob = JSON.parse(process.env.JOB);
        const msg = {
            index : currentJob.index,
            status : JOB_STATUS.Processing
        }

        process.send(JSON.stringify(msg));

        const externalId = getExternalIdFieldName();
        const queryString = `insert into ${hcSchema}.${targetTable.toLowerCase()}(${currentJob.targetColumns}) select ${currentJob.sourceColumns} from ${pcSchema}.${sourceTable} where id between ${currentJob.idFrom} and ${currentJob.idTo} ON CONFLICT (${externalId}) DO NOTHING`;

        let critialError = false;
        try {
            if (!currentJob.shouldCompress) {
                await query(queryString);
            } else {
                const columnsPerRow = currentJob.targetColumnNames.length;
                if (!columnsPerRow) {
                    throw new Error('No target columns found for compressed migration path');
                }

                const rowsPerInsertChunk = Math.max(1, Math.floor(MAX_QUERY_PARAMS / columnsPerRow));
                const cursorChunkSize = Math.max(1, Math.min(rowsPerInsertChunk, MAX_GZIP_CURSOR_CHUNK_ROWS));
                const sourceSelectQuery = `select ${currentJob.sourceColumns} from ${pcSchema}.${sourceTable} where id between ${currentJob.idFrom} and ${currentJob.idTo} order by id`;
                const gzipProgress = {
                    startedAt : Date.now(),
                    sourceRowsRead : 0,
                    rowsInserted : 0,
                    compressedValues : 0,
                    insertChunks : 0,
                    cursorChunks : 0
                };

                console.info(
                    `[GZIP][worker ${process.pid}] job=${currentJob.index} range=${currentJob.idFrom}-${currentJob.idTo} ` +
                    `cursorChunkSize=${cursorChunkSize} insertChunkSize=${rowsPerInsertChunk}`
                );

                await queryCursor(sourceSelectQuery, [], { chunkSize : cursorChunkSize }, async (sourceRows) => {
                    if (!sourceRows?.length) {
                        return;
                    }

                    gzipProgress.cursorChunks++;
                    gzipProgress.sourceRowsRead += sourceRows.length;

                    for (let rowIndex = 0; rowIndex < sourceRows.length; rowIndex += rowsPerInsertChunk) {
                        const rowChunk = sourceRows.slice(rowIndex, rowIndex + rowsPerInsertChunk);
                        const values = rowChunk.flatMap(row =>
                            currentJob.sourceColumnNames.map(sourceColumnName => {
                                const value = row[sourceColumnName] !== undefined
                                    ? row[sourceColumnName]
                                    : row[String(sourceColumnName).toLowerCase()];
                                if (String(sourceColumnName).toLowerCase() === GZIP_COMPRESSED_COLUMN
                                    && value !== null
                                    && value !== undefined) {
                                    gzipProgress.compressedValues++;
                                }
                                return maybeCompressFieldValue(sourceTable, sourceColumnName, value);
                            })
                        );

                        const placeholders = rowChunk.map((_, chunkRowIndex) => {
                            const rowPlaceholders = currentJob.targetColumnNames.map((__, colIndex) =>
                                `$${chunkRowIndex * columnsPerRow + colIndex + 1}`
                            );
                            return `(${rowPlaceholders.join(',')})`;
                        }).join(',');

                        const insertQuery = `insert into ${hcSchema}.${targetTable.toLowerCase()}(${currentJob.targetColumns}) values ${placeholders} ON CONFLICT (${externalId}) DO NOTHING`;
                        await query(insertQuery, values);
                        gzipProgress.rowsInserted += rowChunk.length;
                        gzipProgress.insertChunks++;

                        if (gzipProgress.insertChunks % GZIP_PROGRESS_LOG_EVERY_INSERT_CHUNKS === 0) {
                            const elapsedSeconds = ((Date.now() - gzipProgress.startedAt) / 1000).toFixed(1);
                            console.info(
                                `[GZIP][worker ${process.pid}] job=${currentJob.index} progress ` +
                                `cursorChunks=${gzipProgress.cursorChunks} insertChunks=${gzipProgress.insertChunks} ` +
                                `sourceRowsRead=${gzipProgress.sourceRowsRead} rowsInserted=${gzipProgress.rowsInserted} ` +
                                `compressedValues=${gzipProgress.compressedValues} elapsed=${elapsedSeconds}s`
                            );
                        }
                    }
                });

                const elapsedSeconds = ((Date.now() - gzipProgress.startedAt) / 1000).toFixed(1);
                console.info(
                    `[GZIP][worker ${process.pid}] job=${currentJob.index} completed ` +
                    `cursorChunks=${gzipProgress.cursorChunks} insertChunks=${gzipProgress.insertChunks} ` +
                    `sourceRowsRead=${gzipProgress.sourceRowsRead} rowsInserted=${gzipProgress.rowsInserted} ` +
                    `compressedValues=${gzipProgress.compressedValues} elapsed=${elapsedSeconds}s`
                );
            }
            msg.status = JOB_STATUS.Completed;
        } catch (e) {
            console.error('ERROR: ' + process.pid, { e, currentJob, queryString });
            msg.status = JOB_STATUS.Error;
            critialError = true;
        }

        process.send(JSON.stringify(msg));
        process.exit(critialError ? 1 : 0);
    }

})();
