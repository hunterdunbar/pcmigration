const {
    sourceTable,
    hcSchema : sourceSchema,
    targetTable,
    pcSchema : targetSchema,
    bulkLimit,
    numberOfThreads
} = require('./config/default');

const  { 
    getTableMetadata,
    query,
} = require('./services/db');

const { JOB_STATUS } = require('./services/utils');

const { convertColumnNameToSFformat } = require('./services/converters')
const { processInfoLogging } = require('./services/processInfo');


const cluster = require('cluster');

(async () => {

    if (!targetSchema) {
        throw new Error('Target Schema (HC Schema) is not defined')
    }
    
    if (!sourceSchema) {
        throw new Error('Source Schema (PC Schema) is not defined')
    }

    if (!sourceTable) {
        throw new Error('Source Table is not defined')
    }
    
    if (!targetTable) {
        throw new Error('Target Table is not defined')
    }

    const processInfo = {}

    if (cluster.isMaster) {
        //need to get list of fields from source table
        const tableMetada = await getTableMetadata(sourceSchema, sourceTable);

        //get count of objects
        const countOfRowsInTargetTableRes = await query(`select count(*) from ${targetSchema}.${targetTable}`);
        const countOfRowsInTargetTable = countOfRowsInTargetTableRes?.rows?.[0]?.count;
        const offsetStartsFrom = countOfRowsInTargetTable ? countOfRowsInTargetTable * 1 : 0

        const sourceColumns = tableMetada?.rows.map(r => r.columnName).join(',');
        const targetColumns = tableMetada?.rows.map(r => convertColumnNameToSFformat(r.columnName)).join(',')

        //get count of objects
        const countOfRowsRes = await query(`select count(*) from ${sourceSchema}.${sourceTable}`);
        const countOfRows = countOfRowsRes?.rows?.[0]?.count;

        const countOfJobs =  Math.ceil((1*countOfRows - offsetStartsFrom)/(1 * bulkLimit));
        const queue = [];

        //create list of queries
        for (let i = 0; i < countOfJobs; i++) {
            const offset = (i * bulkLimit) + offsetStartsFrom;
            queue.push({
                index : i,
                status : null,
                sourceColumns,
                targetColumns,
                offset
                // query : 'test'
            })
        }

        processInfo.countOfRecordsToMigrate = countOfRows;
        processInfo.countOfJobs = countOfJobs;
        processInfo.countOfMigratedRecords = countOfRowsInTargetTable;


        if (queue.length === 0) {
            processInfoLogging(processInfo)
            return;
        }

        
        console.log(`Master process ${process.pid} is running`);
      
        for (let i = 0; i < numberOfThreads; i++) {
            const job = queue.find(j => !j.status);
            job.status = JOB_STATUS.Pending;

            cluster.fork({
                JOB : JSON.stringify(job)
            });
        }

        cluster.on('message', (worker, message, handle) => {
            const msg = JSON.parse(message);
            queue[msg.index].status = msg.status;
        });

        cluster.on('exit', (worker, code, signal) => {
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
        });

        const jobMonitor = setInterval(() => {
            processInfo.countOfCompletedJobs = queue.filter(j => j.status === JOB_STATUS.Completed)?.length;
            processInfo.countOfJobsWithError = queue.filter(j => j.status === JOB_STATUS.Error)?.length;
            processInfo.countOfRemainingJobs = processInfo.countOfJobs - processInfo.countOfCompletedJobs - processInfo.countOfJobsWithError;

            processInfoLogging(processInfo);
            if (processInfo.countOfRemainingJobs === 0) {
                clearInterval(jobMonitor);
            }
        }, 10000) //

    } else {
        const currentJob = JSON.parse(process.env.JOB);
        const msg = {
            index : currentJob.index,
            status : JOB_STATUS.Processing
        }

        process.send(JSON.stringify(msg));

        const queryString = `insert into ${targetSchema}.${targetTable}(${currentJob.targetColumns}) select ${currentJob.sourceColumns} from ${sourceSchema}.${sourceTable} order by id limit ${bulkLimit} offset ${currentJob.offset} ON CONFLICT (original_sfid__c) DO NOTHING`
        let critialError = false;
        try {
            await query(queryString);
            msg.status = JOB_STATUS.Completed;
        } catch (e) {
            console.error('ERROR: ' + process.pid, { e, currentJob });
            msg.status = JOB_STATUS.Error;
            critialError = true;
        }

        process.send(JSON.stringify(msg));
        process.exit(critialError ? 1 : 0);
    }

})();