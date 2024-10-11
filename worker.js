require('dotenv').config();
const migrationService = require('./services/migration')

const interval = process.env.WORKER_INTERVAL || 2;

console.debug('[DEBUG]: worker has been started')

function startWorker() {
    setTimeout(async () => {
        console.debug('[DEBUG]: run worker')
        await migrationService.runMigrationProcess()
            .then(() => startWorker())
            .catch(err => console.error('[ERROR]: worker: ', err))
    }, interval * 60000);
}


startWorker()
