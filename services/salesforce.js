const {
    migratedTablePrefix
} = require('./../config/default');
const { convertColumnNameToSFformat } = require('./converters')

function getSalesforceCustomObjectName(tableName) {
    return `${migratedTablePrefix}_${tableName}__c`
}

function getMetadataJson(tableName, tableMetada) {
    return {
        fullName : getSalesforceCustomObjectName(tableName),
        label : `${migratedTablePrefix} ${tableName}`,
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