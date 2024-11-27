require('dotenv').config();

const  { 
    getTableMetadata,
    query,
} = require('./services/db');

const { JOB_STATUS } = require('./services/utils');

const { convertColumnNameToSFformat, getExternalIdFieldName } = require('./services/salesforce')
const { processInfoLogging } = require('./services/processInfo');


const cluster = require('cluster');
const os = require('os');

const sourceTable = process.env.SOURCE_TABLE;
const hcSchema = process.env.HC_SCHEMA || 'salesforce';
const targetTable = process.env.TARGET_TABLE;
const limit = process.env.BULK_LIMIT || 10000;
const numCPUs = process.env.NUMBER_OF_THREADS || os.cpus().length; 

(async () => {

    if (!process.env.CLIENT_DATABASE_URL) {
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
        const tableMetada = await getTableMetadata(sourceTable);

        //get count of objects
        const countOfRowsInTargetTableRes = await query(`select count(*) from ${hcSchema}.${targetTable}`);
        const countOfRowsInTargetTable = countOfRowsInTargetTableRes?.rows?.[0]?.count;

        const sourceColumns = tableMetada?.rows.map(r => r.columnName).join(',');
        const targetColumns = tableMetada?.rows.map(r => convertColumnNameToSFformat(r.columnName)).join(',')

        //get count of objects
        const countOfRowsRes = await query(`select count(*) from ${sourceTable}`);
        const countOfRows = countOfRowsRes?.rows?.[0]?.count;

        const countOfJobs =  Math.ceil((1*countOfRows)/(1 * limit));
        const queue = [];

        //create list of queries
        for (let i = 0; i < countOfJobs; i++) {
            const offset = (i * limit);
            queue.push({
                index : i,
                status : null,
                sourceColumns,
                targetColumns,
                offset
                // query : 'test'
            })
        }

        processInfo.limitPerJob = limit;
        processInfo.countOfRecordsToMigrate = countOfRows;
        processInfo.countOfThreads = numCPUs;
        processInfo.countOfJobs = countOfJobs;
        processInfo.countOfMigratedRecords = countOfRowsInTargetTable;


        if (queue.length === 0) {
            processInfoLogging(processInfo)
            return;
        }

        
        console.log(`Master process ${process.pid} is running`);
      
        for (let i = 0; i < numCPUs; i++) {
            const job = queue.find(j => !j.status);
            job.status = JOB_STATUS.Pending;

            cluster.fork({
                JOB : JSON.stringify(job)
            });
        }

        cluster.on('message', (worker, message) => {
            const msg = JSON.parse(message);
            queue[msg.index].status = msg.status;
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

        //run a query
        // await new Promise(resolve  => {
        //     setTimeout(() => {
        //         resolve();
        //     }, 10000)
        // })
        const externalId = getExternalIdFieldName();
        const queryString = `insert into ${hcSchema}.${targetTable}(${currentJob.targetColumns}) select ${currentJob.sourceColumns} from ${sourceTable} order by id limit ${limit} offset ${currentJob.offset} ON CONFLICT (${externalId}) DO NOTHING`
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