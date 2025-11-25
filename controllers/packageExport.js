
const express = require('express');
const router = express.Router();
const { 
    getTablesInSchemas, 
    buildMaterializedViewWithTablesInfo,
    isMaterializedViewExisting
} = require('./../services/db');

const {
    getMetadataJson, 
    getPermissionSetJson, 
    getPermissionSetName,
    generateCustomObjectFile,
    generatePermissionSetFile,
    generatePackageXmlFile
} = require('./../services/salesforce');

const { pcSchema } = require('../config/default')

const JSZip = require('jszip');

let viewCreationInProgress = false;

async function renderPage(resp, selectedTables = null, errorMessage = null) {

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
            return resp.render('packageExport', { 
                tables : data?.[pcSchema] || [], 
                selectedTables : Array.isArray(selectedTables) ? selectedTables : [ selectedTables ],
                errorMessage
            });
        } else {
            const message = viewCreationInProgress 
                ? 'Analyze of Privacy Center has been started in background. You can continue your work when the process is complete. Refresh the page until the Analyse Privacy Center button disappears.'
                : 'Please hit Analyze Privacy Center button to run the analysis. Refresh the page until this message disappears.'
            return resp.render('packageExport', {
                tables : [], 
                selectedTables : [],
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
    return resp.redirect('/');
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

module.exports = router