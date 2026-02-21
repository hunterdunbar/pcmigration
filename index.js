require('dotenv').config();
const fs = require('fs');
const os = require('os');
const path = require('path');

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
const GZIP_PROGRESS_LOG_EVERY_INSERT_CHUNKS = 10000;
const GZIP_COMPRESSED_COLUMN = 'htmlbody';
const ENABLE_GZIP_PROGRESS_LOG = true;
const PROCESS_INFO_LOG_MIN_INTERVAL_MS = 2000;
const PROC_SELF_CGROUP_PATH = '/proc/self/cgroup';
const CGROUP_V2_BASE_PATH = '/sys/fs/cgroup';
const CGROUP_V1_MEMORY_BASE_PATH = '/sys/fs/cgroup/memory';
const MAX_GZIP_BATCH_BYTES = (() => {
    const configuredMaxBatchBytes = Number(process.env.MAX_GZIP_BATCH_BYTES);
    return Number.isFinite(configuredMaxBatchBytes) && configuredMaxBatchBytes > 0
        ? Math.floor(configuredMaxBatchBytes)
        : null; // no hard cap by default; bounded by available memory
})();
const MIN_GZIP_BATCH_BYTES = 8 * 1024 * 1024; // 8MB floor
const DEFAULT_GZIP_BATCH_BYTES = 32 * 1024 * 1024; // safe fallback
const GZIP_AVAILABLE_MEMORY_FRACTION = 0.35;
const GZIP_MEMORY_PRESSURE_FRACTION = 0.85;
const GZIP_MEMORY_RECHECK_EVERY_INSERT_CHUNKS = 5;
const GZIP_ROW_MEMORY_OVERHEAD_FACTOR = 2;
const DEFAULT_GZIP_ROW_BYTES_ESTIMATE = 1024 * 1024;
const CGROUP_UNLIMITED_THRESHOLD_BYTES = 1024 * 1024 * 1024 * 1024;
const HEROKU_FALLBACK_MEMORY_LIMIT_BYTES = 512 * 1024 * 1024;

function readTextFileSafe(filePath) {
    try {
        return fs.readFileSync(filePath, 'utf8').trim();
    } catch (_) {
        return null;
    }
}

function parseMemoryValue(value) {
    if (!value || value === 'max') {
        return null;
    }

    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0 || parsed >= CGROUP_UNLIMITED_THRESHOLD_BYTES) {
        return null;
    }
    return parsed;
}

function parseCgroupPaths() {
    const content = readTextFileSafe(PROC_SELF_CGROUP_PATH);
    if (!content) {
        return {
            v2RelativePath : null,
            v1MemoryRelativePath : null
        };
    }

    let v2RelativePath = null;
    let v1MemoryRelativePath = null;
    content.split('\n').forEach(line => {
        const [hierarchy, controllers, cgroupRelativePath] = line.split(':');
        if (hierarchy === '0' && controllers === '') {
            v2RelativePath = cgroupRelativePath;
            return;
        }

        if (controllers?.split(',').includes('memory')) {
            v1MemoryRelativePath = cgroupRelativePath;
        }
    });

    return {
        v2RelativePath,
        v1MemoryRelativePath
    };
}

function toCgroupFilePath(basePath, cgroupRelativePath, fileName) {
    if (!cgroupRelativePath || cgroupRelativePath === '/') {
        return path.posix.join(basePath, fileName);
    }
    return path.posix.join(basePath, cgroupRelativePath.replace(/^\/+/, ''), fileName);
}

const CGROUP_PATHS = parseCgroupPaths();

function getMemorySnapshot() {
    const rssBytes = Number(process.memoryUsage()?.rss) || 0;

    const v2LimitPath = toCgroupFilePath(CGROUP_V2_BASE_PATH, CGROUP_PATHS.v2RelativePath, 'memory.max');
    const v2UsagePath = toCgroupFilePath(CGROUP_V2_BASE_PATH, CGROUP_PATHS.v2RelativePath, 'memory.current');
    const v2LimitBytes = parseMemoryValue(readTextFileSafe(v2LimitPath));
    const v2UsageBytes = parseMemoryValue(readTextFileSafe(v2UsagePath));
    if (v2LimitBytes && v2UsageBytes !== null) {
        return {
            source : 'cgroup-v2',
            limitBytes : v2LimitBytes,
            usageBytes : v2UsageBytes,
            rssBytes
        };
    }

    const v1LimitPath = toCgroupFilePath(CGROUP_V1_MEMORY_BASE_PATH, CGROUP_PATHS.v1MemoryRelativePath, 'memory.limit_in_bytes');
    const v1UsagePath = toCgroupFilePath(CGROUP_V1_MEMORY_BASE_PATH, CGROUP_PATHS.v1MemoryRelativePath, 'memory.usage_in_bytes');
    const v1LimitBytes = parseMemoryValue(readTextFileSafe(v1LimitPath));
    const v1UsageBytes = parseMemoryValue(readTextFileSafe(v1UsagePath));
    if (v1LimitBytes && v1UsageBytes !== null) {
        return {
            source : 'cgroup-v1',
            limitBytes : v1LimitBytes,
            usageBytes : v1UsageBytes,
            rssBytes
        };
    }

    const isHerokuDyno = Boolean(process.env.DYNO);
    const osTotalMemory = Number(os.totalmem()) || null;
    const fallbackLimitBytes = isHerokuDyno
        ? Math.min(osTotalMemory || HEROKU_FALLBACK_MEMORY_LIMIT_BYTES, HEROKU_FALLBACK_MEMORY_LIMIT_BYTES)
        : osTotalMemory;

    return {
        source : 'os-totalmem',
        limitBytes : fallbackLimitBytes,
        usageBytes : rssBytes,
        rssBytes
    };
}

function calculateSafeBatchBytes(memorySnapshot) {
    const limitBytes = Number(memorySnapshot?.limitBytes);
    const usageBytes = Number(memorySnapshot?.usageBytes);
    const configuredWorkers = Number(numberOfThreads);
    const workerCount = Number.isFinite(configuredWorkers) && configuredWorkers > 0
        ? Math.floor(configuredWorkers)
        : 1;

    if (Number.isFinite(limitBytes) && Number.isFinite(usageBytes) && limitBytes > usageBytes) {
        const availableBytes = Math.max(1, limitBytes - usageBytes);
        const perWorkerAvailableBytes = Math.max(1, Math.floor(availableBytes / workerCount));
        const preferredBytes = Math.floor(perWorkerAvailableBytes * GZIP_AVAILABLE_MEMORY_FRACTION);
        const hardLimitBytes = Number.isFinite(MAX_GZIP_BATCH_BYTES) && MAX_GZIP_BATCH_BYTES > 0
            ? MAX_GZIP_BATCH_BYTES
            : perWorkerAvailableBytes;
        const maxAllowedBytes = Math.min(hardLimitBytes, perWorkerAvailableBytes);
        const minAllowedBytes = Math.min(MIN_GZIP_BATCH_BYTES, maxAllowedBytes);
        const safeBytes = Math.max(minAllowedBytes, Math.min(preferredBytes, maxAllowedBytes));
        return Math.max(1, safeBytes);
    }

    return DEFAULT_GZIP_BATCH_BYTES;
}

function isMemoryPressureHigh(memorySnapshot) {
    const limitBytes = Number(memorySnapshot?.limitBytes);
    const usageBytes = Number(memorySnapshot?.usageBytes);
    const rssBytes = Number(memorySnapshot?.rssBytes);

    if (!Number.isFinite(limitBytes) || limitBytes <= 0) {
        return false;
    }

    const usageRatio = Number.isFinite(usageBytes) ? (usageBytes / limitBytes) : 0;
    const rssRatio = Number.isFinite(rssBytes) ? (rssBytes / limitBytes) : 0;
    return usageRatio >= GZIP_MEMORY_PRESSURE_FRACTION || rssRatio >= GZIP_MEMORY_PRESSURE_FRACTION;
}

function estimateValueBytes(value) {
    if (value === null || value === undefined) {
        return 0;
    }
    if (Buffer.isBuffer(value)) {
        return value.length;
    }
    return Buffer.byteLength(String(value), 'utf8');
}

function calculateSafeCursorChunkRows(rowsPerInsertChunk, dynamicBatchBytes, estimatedRowBytes) {
    const normalizedRowEstimate = Math.max(1, Number(estimatedRowBytes) || DEFAULT_GZIP_ROW_BYTES_ESTIMATE);
    const safeRowBytes = Math.max(
        DEFAULT_GZIP_ROW_BYTES_ESTIMATE,
        Math.floor(normalizedRowEstimate * GZIP_ROW_MEMORY_OVERHEAD_FACTOR)
    );
    const memoryBoundRows = Math.max(1, Math.floor(dynamicBatchBytes / safeRowBytes));

    return Math.max(
        1,
        Math.min(rowsPerInsertChunk, MAX_GZIP_CURSOR_CHUNK_ROWS, memoryBoundRows)
    );
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
        const shouldCompress = shouldCompressTable(sourceTable);
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
                const columnsPerRow = currentJob.targetColumnNames.length;
                if (!columnsPerRow) {
                    throw new Error('No target columns found for compressed migration path');
                }

                const rowsPerInsertChunk = Math.max(1, Math.floor(MAX_QUERY_PARAMS / columnsPerRow));
                let memorySnapshot = getMemorySnapshot();
                let dynamicBatchBytes = calculateSafeBatchBytes(memorySnapshot);
                let estimatedRowBytes = DEFAULT_GZIP_ROW_BYTES_ESTIMATE;
                let cursorChunkSize = calculateSafeCursorChunkRows(
                    rowsPerInsertChunk,
                    dynamicBatchBytes,
                    estimatedRowBytes
                );
                const sourceSelectQuery = `select ${currentJob.sourceColumns} from ${pcSchema}.${sourceTable} where id between ${currentJob.idFrom} and ${currentJob.idTo} order by id`;
                const gzipProgress = {
                    startedAt : Date.now(),
                    sourceRowsRead : 0,
                    rowsInserted : 0,
                    compressedValues : 0,
                    insertChunks : 0,
                    cursorChunks : 0
                };
                let pendingRows = [];
                let pendingBytes = 0;

                if (ENABLE_GZIP_PROGRESS_LOG) {
                    console.info(
                        `[GZIP][worker ${process.pid}] job=${currentJob.index} range=${currentJob.idFrom}-${currentJob.idTo} ` +
                        `cursorChunkSize=${cursorChunkSize} insertChunkSize=${rowsPerInsertChunk} ` +
                        `batchBytes=${dynamicBatchBytes} memorySource=${memorySnapshot.source}`
                    );
                }

                const flushPendingRows = async () => {
                    if (!pendingRows.length) {
                        return;
                    }

                    const values = pendingRows.flat();
                    const placeholders = pendingRows.map((_, chunkRowIndex) => {
                        const rowPlaceholders = currentJob.targetColumnNames.map((__, colIndex) =>
                            `$${chunkRowIndex * columnsPerRow + colIndex + 1}`
                        );
                        return `(${rowPlaceholders.join(',')})`;
                    }).join(',');

                    const insertQuery = `insert into ${hcSchema}.${targetTable.toLowerCase()}(${currentJob.targetColumns}) values ${placeholders} ON CONFLICT (${externalId}) DO NOTHING`;
                    const insertResult = await query(insertQuery, values);
                    insertedCount += Number(insertResult?.rowCount) || 0;
                    gzipProgress.rowsInserted += pendingRows.length;
                    gzipProgress.insertChunks++;
                    pendingRows = [];
                    pendingBytes = 0;

                    const shouldRecheckMemory = gzipProgress.insertChunks % GZIP_MEMORY_RECHECK_EVERY_INSERT_CHUNKS === 0;
                    if (shouldRecheckMemory) {
                        memorySnapshot = getMemorySnapshot();
                        dynamicBatchBytes = calculateSafeBatchBytes(memorySnapshot);
                    } else if (isMemoryPressureHigh(memorySnapshot)) {
                        const refreshedMemorySnapshot = getMemorySnapshot();
                        memorySnapshot = refreshedMemorySnapshot;
                        const safeBatchBytes = calculateSafeBatchBytes(refreshedMemorySnapshot);
                        dynamicBatchBytes = Math.max(
                            MIN_GZIP_BATCH_BYTES,
                            Math.floor(safeBatchBytes * 0.5)
                        );
                    }

                    if (ENABLE_GZIP_PROGRESS_LOG
                        && gzipProgress.insertChunks % GZIP_PROGRESS_LOG_EVERY_INSERT_CHUNKS === 0) {
                        const elapsedSeconds = ((Date.now() - gzipProgress.startedAt) / 1000).toFixed(1);
                        console.info(
                            `[GZIP][worker ${process.pid}] job=${currentJob.index} progress ` +
                            `cursorChunks=${gzipProgress.cursorChunks} insertChunks=${gzipProgress.insertChunks} ` +
                            `sourceRowsRead=${gzipProgress.sourceRowsRead} rowsInserted=${gzipProgress.rowsInserted} ` +
                            `compressedValues=${gzipProgress.compressedValues} batchBytes=${dynamicBatchBytes} ` +
                            `memUsage=${memorySnapshot.usageBytes} memLimit=${memorySnapshot.limitBytes} elapsed=${elapsedSeconds}s`
                        );
                    }
                };

                const getDynamicCursorChunkSize = () => {
                    const snapshot = getMemorySnapshot();
                    const safeBatchBytes = calculateSafeBatchBytes(snapshot);
                    const adjustedBatchBytes = isMemoryPressureHigh(snapshot)
                        ? Math.max(MIN_GZIP_BATCH_BYTES, Math.floor(safeBatchBytes * 0.5))
                        : safeBatchBytes;
                    const nextCursorChunkSize = calculateSafeCursorChunkRows(
                        rowsPerInsertChunk,
                        adjustedBatchBytes,
                        estimatedRowBytes
                    );
                    cursorChunkSize = nextCursorChunkSize;
                    return nextCursorChunkSize;
                };

                await queryCursor(sourceSelectQuery, [], { chunkSize : cursorChunkSize, getChunkSize : getDynamicCursorChunkSize }, async (sourceRows) => {
                    if (!sourceRows?.length) {
                        return;
                    }

                    memorySnapshot = getMemorySnapshot();
                    dynamicBatchBytes = calculateSafeBatchBytes(memorySnapshot);
                    if (isMemoryPressureHigh(memorySnapshot)) {
                        dynamicBatchBytes = Math.max(
                            MIN_GZIP_BATCH_BYTES,
                            Math.floor(dynamicBatchBytes * 0.5)
                        );
                    }

                    gzipProgress.cursorChunks++;
                    gzipProgress.sourceRowsRead += sourceRows.length;

                    for (let rowIndex = 0; rowIndex < sourceRows.length; rowIndex++) {
                        const row = sourceRows[rowIndex];
                        const transformedRow = [];
                        let rowBytes = 0;
                        let sourceRowBytes = 0;

                        for (let colIndex = 0; colIndex < currentJob.sourceColumnNames.length; colIndex++) {
                            const sourceColumnName = currentJob.sourceColumnNames[colIndex];
                            const value = row[sourceColumnName] !== undefined
                                ? row[sourceColumnName]
                                : row[String(sourceColumnName).toLowerCase()];
                            sourceRowBytes += estimateValueBytes(value);
                            if (String(sourceColumnName).toLowerCase() === GZIP_COMPRESSED_COLUMN
                                && value !== null
                                && value !== undefined) {
                                gzipProgress.compressedValues++;
                            }

                            const mappedValue = maybeCompressFieldValue(sourceTable, sourceColumnName, value);
                            transformedRow.push(mappedValue);
                            rowBytes += estimateValueBytes(mappedValue);
                        }

                        rowBytes += (columnsPerRow * 8) + 64;
                        const observedRowBytes = Math.max(sourceRowBytes, rowBytes);
                        estimatedRowBytes = Math.max(
                            1,
                            Math.floor((estimatedRowBytes * 0.85) + (observedRowBytes * 0.15))
                        );

                        if (isMemoryPressureHigh(memorySnapshot) && pendingRows.length) {
                            await flushPendingRows();
                        }

                        const wouldExceedRowsLimit = pendingRows.length >= rowsPerInsertChunk;
                        const wouldExceedBytesLimit = pendingRows.length > 0 && (pendingBytes + rowBytes > dynamicBatchBytes);
                        if (wouldExceedRowsLimit || wouldExceedBytesLimit) {
                            await flushPendingRows();
                        }

                        pendingRows.push(transformedRow);
                        pendingBytes += rowBytes;

                        const shouldFlushByRows = pendingRows.length >= rowsPerInsertChunk;
                        const shouldFlushByBytes = pendingBytes >= dynamicBatchBytes;
                        const isSingleOversizedRow = pendingRows.length === 1 && rowBytes > dynamicBatchBytes;
                        if (shouldFlushByRows || shouldFlushByBytes || isSingleOversizedRow) {
                            await flushPendingRows();
                        }
                    }
                });
                await flushPendingRows();

                if (ENABLE_GZIP_PROGRESS_LOG) {
                    const elapsedSeconds = ((Date.now() - gzipProgress.startedAt) / 1000).toFixed(1);
                    console.info(
                        `[GZIP][worker ${process.pid}] job=${currentJob.index} completed ` +
                        `cursorChunks=${gzipProgress.cursorChunks} insertChunks=${gzipProgress.insertChunks} ` +
                        `sourceRowsRead=${gzipProgress.sourceRowsRead} rowsInserted=${gzipProgress.rowsInserted} ` +
                        `compressedValues=${gzipProgress.compressedValues} elapsed=${elapsedSeconds}s`
                    );
                }
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
