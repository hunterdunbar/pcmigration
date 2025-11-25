
const express = require('express');
const router = express.Router();
const { validateSession } = require('./../services/security');
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

async function renderPage(resp, selectedTables = null, errorMessage = null, message = null) {

    if (!pcSchema) {
        return resp.render('packageExport', { 
            tables : [], 
            selectedTables : [],
            errorMessage : 'Privacy Center schema (PC_SCHEMA) is not defined',
            message,
            showAnalyzeButton: false
        });
    }

    const isViewExisting = await isMaterializedViewExisting();

    try {
        let data = await getTablesInSchemas([ pcSchema ]);
        return resp.render('packageExport', { 
            tables : data?.[pcSchema] || [], 
            selectedTables : Array.isArray(selectedTables) ? selectedTables : [ selectedTables ],
            errorMessage,
            message : !isViewExisting ? 'Please hit Analyze Privacy Center button to run the analysis. Refresh the page until the Analyse Privacy Center button disappears.' : message,
            showAnalyzeButton: !isViewExisting
        });
    } catch (e) {
        console.error('error:', e);
        return resp.render('packageExport', { 
            tables : [], 
            selectedTables : [],
            errorMessage : e.message  || e,
            message,
            showAnalyzeButton: !isViewExisting
        });
    }
}

router.get('/packageExport', validateSession(), async (req, resp) => {
    return renderPage(resp)
})

router.get('/generatePackageXml', (req, resp) => {
    resp.redirect('/packageExport')
})

router.post('/generatePackageXml', validateSession(), async (req, resp) => {

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

router.post('/analyzePrivacyCenter', validateSession(), async (req, resp) => {
    //don't need to await here
    buildMaterializedViewWithTablesInfo();
    
    return renderPage(resp, null, null, 'Analyze of Privacy Center has been started in background. You can continue your work when the process is complete. Refresh the page until the Analyse Privacy Center button disappears.');

})

module.exports = router