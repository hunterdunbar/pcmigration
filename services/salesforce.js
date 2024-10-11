const { convertColumnNameToSFformat } = require('./converters')

const MIGRATED_SF_OBJECT_PREFIX = process.env.MIGRATED_OBJECT_PREFIX || 'pcma';


function getSalesforceCustomObjectName(tableName) {
    return `${MIGRATED_SF_OBJECT_PREFIX}_${tableName}__c`
}

function getMetadataJson(tableName, tableMetada) {
    return {
        fullName : getSalesforceCustomObjectName(tableName),
        label : `${MIGRATED_SF_OBJECT_PREFIX} ${tableName}`,
        pluralLabel : tableName,
        deploymentStatus: 'Deployed',
        sharingModel: 'ReadWrite',
        nameField: {
            type: 'AutoNumber',
            label: 'Auto Number'
        },
        fields : tableMetada?.rows?.map(row => {
            const sfField = {
                fullName : convertColumnNameToSFformat(row.columnName),
                label : row.columnName,
                type : row.length > 255 ? 'LongTextArea' : 'Text',
                length : row.length || 255,
                externalId : row.columnName === 'sfid',
                unique : row.columnName === 'sfid',
            };

            if (sfField.type === 'LongTextArea') {
                sfField.visibleLines = 3;
            }

            return sfField;
        })
    }
}

const PERMISION_SET_NAME_TEMPLATE = 'PCMA Permission Set For';

function getPermissionSetJson(objectMetadata) {
    return {
        label : `${PERMISION_SET_NAME_TEMPLATE} ${objectMetadata.fullName}`,
        fieldPermissions : objectMetadata.fields.map(field => {
            return {
                editable : true,
                readable : true,
                field : `${objectMetadata.fullName}.${field.fullName}`
            }
        }),
        objectPermissions : {
            allowCreate : true,
            allowDelete : true,
            allowEdit : true,
            allowRead : true,
            modifyAllRecords : true,
            object : objectMetadata.fullName,
            viewAllRecords : true
        }
    }
}

function getPermissionSetName(objectName) {
    return `${PERMISION_SET_NAME_TEMPLATE} ${objectName}`.replaceAll(' ', '_');
}

module.exports = {
    getMetadataJson,
    getPermissionSetJson,
    getPermissionSetName
}