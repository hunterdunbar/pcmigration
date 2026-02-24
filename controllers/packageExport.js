
const express = require('express');
const router = express.Router();
const fs = require('fs');
const os = require('os');
const { 
    getTablesInSchemas, 
    buildMaterializedViewWithTablesInfo,
    isMaterializedViewExisting,
    dropMaterializedView,
    query
} = require('./../services/db');

const {
    getMetadataJson, 
    getPermissionSetJson, 
    getPermissionSetName,
    generateCustomObjectFile,
    generatePermissionSetFile,
    generatePackageXmlFile
} = require('./../services/salesforce');

const { pcSchema, numberOfThreads, bulkLimit } = require('../config/default')

const JSZip = require('jszip');

let viewCreationInProgress = false;

const EMAILMESSAGE_TABLE = 'emailmessage';
const HTMLBODY_COLUMN = 'htmlbody';
const PCMA_VIEW_NAME = 'pcma_tables_info_mv';
const MEMORY_UNLIMITED_THRESHOLD_BYTES = 1024 * 1024 * 1024 * 1024;
const BULK_MEMORY_FRACTION = 0.75;
const GZIP_MEMORY_OVERHEAD_FACTOR = 1.5;
const MEMORY_PER_THREAD_MB = 512;

function normalizeSelectedTables(selectedTables) {
    if (Array.isArray(selectedTables)) {
        return selectedTables;
    }
    if (selectedTables === null || selectedTables === undefined || selectedTables === '') {
        return [];
    }
    return [ selectedTables ];
}

function parseMemoryLimitBytes(value) {
    const parsed = Number(String(value || '').trim());
    if (!Number.isFinite(parsed) || parsed <= 0 || parsed >= MEMORY_UNLIMITED_THRESHOLD_BYTES) {
        return null;
    }
    return Math.floor(parsed);
}

function readTextFileSafe(filePath) {
    try {
        return fs.readFileSync(filePath, 'utf8').trim();
    } catch (e) {
        return null;
    }
}

function getRuntimeMemoryLimitBytes() {
    const cgroupV2Limit = parseMemoryLimitBytes(readTextFileSafe('/sys/fs/cgroup/memory.max'));
    if (cgroupV2Limit) {
        return cgroupV2Limit;
    }

    const cgroupV1Limit = parseMemoryLimitBytes(readTextFileSafe('/sys/fs/cgroup/memory/memory.limit_in_bytes'));
    if (cgroupV1Limit) {
        return cgroupV1Limit;
    }

    const total = Number(os.totalmem());
    return Number.isFinite(total) && total > 0 ? total : null;
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function roundToStep(value, step) {
    return Math.round(value / step) * step;
}

async function getEmailMessageRecommendation(tables = []) {
    const emailTableInfo = tables.find((table) => String(table?.tablename || '').toLowerCase() === EMAILMESSAGE_TABLE);
    if (!emailTableInfo) {
        return null;
    }

    const maxLengthRes = await query(
        `select column_size::bigint as max_length
         from ${pcSchema}.${PCMA_VIEW_NAME}
         where table_name = $1 and column_name = $2
         limit 1`,
        [EMAILMESSAGE_TABLE, HTMLBODY_COLUMN]
    );
    const maxHtmlBodyLength = Math.max(1, Number(maxLengthRes?.rows?.[0]?.max_length) || 1);

    const memoryLimitBytes = getRuntimeMemoryLimitBytes();
    const memoryLimitMb = memoryLimitBytes ? Math.round(memoryLimitBytes / (1024 * 1024)) : null;
    const currentThreadCount = Math.max(1, Number(numberOfThreads) || 1);
    const currentBulkLimit = Math.max(1, Number(bulkLimit) || 1);
    const suggestedThreadCount = memoryLimitMb
        ? clamp(Math.floor(memoryLimitMb / MEMORY_PER_THREAD_MB), 1, 8)
        : currentThreadCount;

    const effectiveMemoryBytes = memoryLimitBytes || (1024 * 1024 * 1024);
    const perWorkerBudgetBytes = Math.max(1, Math.floor((effectiveMemoryBytes * BULK_MEMORY_FRACTION) / suggestedThreadCount));
    const estimatedMemoryPerRowBytes = Math.max(1024, Math.floor(maxHtmlBodyLength * GZIP_MEMORY_OVERHEAD_FACTOR));
    const rawRecommendedBulkLimit = clamp(
        Math.floor(perWorkerBudgetBytes / estimatedMemoryPerRowBytes),
        64,
        5000
    );
    const recommendedBulkLimit = clamp(roundToStep(rawRecommendedBulkLimit, 10), 60, 5000);

    return {
        tableName : EMAILMESSAGE_TABLE,
        maxHtmlBodyLength,
        maxHtmlBodyLengthKb : Math.max(1, Math.round(maxHtmlBodyLength / 1024)),
        memoryLimitMb,
        recommendedBulkLimit,
        currentThreadCount,
        suggestedThreadCount,
        currentBulkLimit
    };
}

async function renderPage(resp, selectedTables = null, errorMessage = null) {
    const normalizedSelectedTables = normalizeSelectedTables(selectedTables);

    if (!pcSchema) {
        return resp.render('packageExport', { 
            tables : [], 
            selectedTables : [],
            errorMessage : 'Privacy Center schema (PC_SCHEMA) is not defined'
        });
    }

    const isViewExisting = await isMaterializedViewExisting();

    try {
        if (isViewExisting) {
            let data = await getTablesInSchemas([ pcSchema ]);
            const tables = data?.[pcSchema] || [];
            const emailMessageRecommendation = await getEmailMessageRecommendation(tables);
            return resp.render('packageExport', { 
                tables, 
                selectedTables : normalizedSelectedTables,
                emailMessageRecommendation,
                errorMessage,
                showRefreshButton : true
            });
        } else {
            const message = viewCreationInProgress 
                ? 'Analyze of Privacy Center has been started in background. You can continue your work when the process is complete. Refresh the page until the Analyse Privacy Center button disappears.'
                : 'Please hit Analyze Privacy Center button to run the analysis. Refresh the page until this message disappears.'
            return resp.render('packageExport', {
                tables : [], 
                selectedTables : [],
                emailMessageRecommendation : null,
                errorMessage,
                message,
                showAnalyzeButton : !viewCreationInProgress
            });
        }
    } catch (e) {
        console.error('error:', e);
        return resp.render('packageExport', { 
            tables : [], 
            selectedTables : [],
            emailMessageRecommendation : null,
            errorMessage : e.message  || e
        });
    }
}

router.get('/packageExport', async (req, resp) => {
    return renderPage(resp)
})

router.get('/generatePackageXml', (req, resp) => {
    return resp.redirect('/packageExport')
})

router.post('/generatePackageXml', async (req, resp) => {
    const { selectedTables, includePermissonSet } = req.body;

    if (!selectedTables?.length) {
        return renderPage(resp, selectedTables, 'Table is not selected')
    } else {

        try {
            const metadata = await Promise.all(
                (Array.isArray(selectedTables) ? selectedTables : [ selectedTables ])
                    .map(tableName => getMetadataJson(pcSchema, tableName))
            )

            if (!metadata?.length) {
                return renderPage(resp, selectedTables, 'Table Info not found in schema ' + pcSchema)
            }

            const zip = new JSZip();
        
            const objectsFolder = zip.folder('objects');
            metadata.forEach(m => objectsFolder.file(`${m.fullName}.object`, generateCustomObjectFile(m)));
            if (includePermissonSet) {
                const permissionSetFolder = zip.folder('permissionsets');
                metadata.forEach(m => 
                    permissionSetFolder.file(`${getPermissionSetName(m.label)}.permissionset`, 
                        generatePermissionSetFile(getPermissionSetJson(m))));
            }

            const packageXml = generatePackageXmlFile({ 
                objectNames : metadata.map(m => m.fullName),
                permissionSets : includePermissonSet 
                    ? metadata.map(m => getPermissionSetName(m.label)) 
                    : null
            });

            zip.file('package.xml', packageXml);

            const fileName = `objects_for_deploy.zip`;

            resp.attachment(fileName);

            zip.generateNodeStream({ 
                    type: 'nodebuffer', 
                    streamFiles: true, 
                    compression : 'DEFLATE',
                })
                .pipe(resp)
                .on('finish', () => {
                    console.log(`${fileName} saved`);
                })
                .on('error', (err) => {
                    return renderPage(resp, selectedTables, err.message)
                })
            
        } catch (e) {
            console.error('error:', e);
            return renderPage(resp, selectedTables, e.message || e || 'Something goes wrong, check log file');
        }
    }
})

router.get('/analyzePrivacyCenter', (req, resp) => {
    return resp.redirect('/packageExport');
})


router.post('/analyzePrivacyCenter', (req, resp) => {
    //don't need to await result here
    if (!viewCreationInProgress) {
        viewCreationInProgress = true;
        buildMaterializedViewWithTablesInfo()
            .catch(err => {
                console.error('Error during materialized view creation:', err);
            }).finally(() => {
                viewCreationInProgress = false;
            });

    }
    return resp.redirect('/packageExport');
    
})

router.get('/refreshMaterializedView', (req, resp) => {
    return resp.redirect('/packageExport');
})

router.post('/refreshMaterializedView', async (req, resp) => {
    try {
        await dropMaterializedView();
        return resp.redirect('/packageExport');
    } catch (e) {
        console.error('Error dropping materialized view:', e);
        return renderPage(resp, null, e.message || e || 'Failed to refresh materialized view');
    }
})

module.exports = router
