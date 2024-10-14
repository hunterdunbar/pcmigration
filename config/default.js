require('dotenv').config();
const os = require('os');

module.exports = {
    clientDbUrl : process.env.CLIENT_DATABASE_URL,
    hcSchema : process.env.HC_SCHEMA || 'salesforce',
    pcSchema : process.env.PC_SCHEMA || 'cache',
    sourceTable : process.env.SOURCE_TABLE,
    targetTable : process.env.TARGET_TABLE,
    migratedTablePrefix : process.env.MIGRATED_OBJECT_PREFIX || 'pcma',
    bulkLimit : process.env.BULK_LIMIT || 10000,
    numberOfThreads : process.env.NUMBER_OF_THREADS || os.cpus().length,
    appPassword : process.env.APP_PASS || null,
    appUsername : process.env.APP_USERNAME || null,
}