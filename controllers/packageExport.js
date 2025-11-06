
const express = require('express');
const router = express.Router();
const { validateSession } = require('./../services/security');
const { getTablesInSchemas } = require('./../services/db');
const { getMetadataJson, getPermissionSetJson, getPermissionSetName } = require('./../services/salesforce');

const xmlConverter = require('xml-js');
const xmlConverterOptions = {compact: true, ignoreComment: true, spaces: 4};

const { pcSchema } = require('../config/default')

const JSZip = require('jszip');

async function renderPage(resp, selectedTables = null, errorMessage = null) {

    if (!pcSchema) {
        return resp.render('packageExport', { 
            tables : [], 
            selectedTables : [],
            errorMessage : 'Privacy Center schema (PC_SCHEMA) is not defined'
        });
    }

    try {
        let data = await getTablesInSchemas([ pcSchema ]);
        return resp.render('packageExport', { 
            tables : data?.[pcSchema] || [], 
            selectedTables : Array.isArray(selectedTables) ? selectedTables : [ selectedTables ],
            errorMessage 
        });
    } catch (e) {
        console.error('error:', e);
        return resp.render('packageExport', { 
            tables : [], 
            selectedTables : [],
            errorMessage : e.message  || e
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
                    return renderPage(resp, selectedTables, 'Ok ' + selectedTables.join(', '), err.message)
                })
            
        } catch (e) {
            console.error('error:', e);
            return renderPage(resp, selectedTables, e.message || e || 'Something goes wrong, check log file');
        }
    }
})

function generateCustomObjectFile(metadataJson) {
    return convertToXml({
        "_declaration" : { 
            "_attributes" : { "version" : "1.0" , "encoding":"utf-8" } 
        },
        CustomObject : {
            "_attributes": { "xmlns" : "http://soap.sforce.com/2006/04/metadata" },
            ...metadataJson
        }
    })
}

function generatePermissionSetFile(metadataJson) {
    return convertToXml({
        "_declaration" : { 
            "_attributes" : { "version" : "1.0" , "encoding":"utf-8" } 
        },
        PermissionSet : {
            "_attributes": { "xmlns" : "http://soap.sforce.com/2006/04/metadata" },
            ...metadataJson
        }
    })
}


function generatePackageXmlFile({ objectNames = [], permissionSets = [] }) {
    const packageXmlJson = {
        "_declaration" : { 
            "_attributes" : { "version" : "1.0" , "encoding":"UTF-8" } 
        },
        Package : {
            "_attributes": { "xmlns" : "http://soap.sforce.com/2006/04/metadata" },
            types : [        
                { 
                    members : objectNames,
                    name : 'CustomObject'
                }
            ],
            version : '55.0'
        }
    }

    if (permissionSets?.length) {
        packageXmlJson.Package.types = [
            ...packageXmlJson.Package.types,
            {   
                members : permissionSets,
                name : 'PermissionSet'
            }
        ]
    }

    return convertToXml(packageXmlJson)
}


function convertToXml(jsonObject) {
    return xmlConverter.js2xml(jsonObject, xmlConverterOptions)
}

module.exports = router