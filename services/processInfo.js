const {
    sourceTable,
    hcSchema : sourceSchema,
    targetTable,
    pcSchema : targetSchema,
    bulkLimit,
    numberOfThreads
} = require('./../config/default');

function processInfoLogging(processInfo) {

    console.debug(`
        

        ############## PCMA Process Info ##############
        Datetime: ${new Date().toLocaleString()}

        Source Table: ${sourceSchema}.${sourceTable}
        Target Table: ${targetSchema}.${targetTable}
        Count of Records Must be Migrated: ${processInfo.countOfRecordsToMigrate || 0}
        Count of Records Already Migrated: ${processInfo.countOfMigratedRecords || 0}

        Status: ${processInfo.countOfRemainingJobs ? 'In Progress' : 'Completed'}
        Count of Threads (CPUs): ${numberOfThreads}
        Limit per Job (Bulk Limit): ${bulkLimit}
        Total Count of Jobs: ${processInfo.countOfJobs || 0}

        Count of Completed Jobs: ${processInfo.countOfCompletedJobs || 'N/A'}
        Count of Remaining Jobs: ${processInfo.countOfRemainingJobs || 'N/A'}
        
        Count of Jobs With Error: ${processInfo.countOfJobsWithError || 'N/A'}


        ############## PCMA Process Info ##############


    `)
}

module.exports = {
    processInfoLogging
}