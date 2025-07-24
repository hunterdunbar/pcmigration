require('dotenv').config();
const os = require('os');

module.exports = {
    clientDbUrl : process.env.CLIENT_DATABASE_URL,
    hcSchema : process.env.HC_SCHEMA?.toLowerCase() || 'salesforce',
    pcSchema : process.env.PC_SCHEMA?.toLowerCase() || 'cache',
    sourceTable : process.env.SOURCE_TABLE?.toLowerCase(),
    targetTable : process.env.TARGET_TABLE?.toLowerCase(),
    bulkLimit : process.env.BULK_LIMIT || 10000,
    numberOfThreads : process.env.NUMBER_OF_THREADS || os.cpus().length,
    appPassword : process.env.APP_PASS || null,
    appUsername : process.env.APP_USERNAME || null,
    migratedTablePrefix : process.env.MIGRATED_STANDRD_OBJECT_PREFIX || 'migrated',
    migratedCustomTablePrefix : process.env.MIGRATED_CUSTOM_OBJECT_PREFIX || 'migrated_custom',
    useLongTextLength : process.env.USE_LONG_TEXT_LENGTH || false,
}