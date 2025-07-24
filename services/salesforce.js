
const { 
    migratedCustomTablePrefix, 
    migratedTablePrefix,
    useLongTextAreaFieldType
} = require('./../config/default');

const { getColumns } = require('./db');


function getSalesforceCustomObjectName(tableName) {
    tableName = tableName.replace(/(.)__([^c].)/gi, '$1_$2');
    if (tableName.endsWith('__c')) {
        return `${migratedCustomTablePrefix}_${tableName}`;
    }
    return `${migratedTablePrefix}_${tableName}__c`;
}

function getExternalIdFieldName() {
    return 'id__c';
}

function convertColumnNameToSFformat(columnName) {
    if (columnName === 'sfid') {
        return getExternalIdFieldName();
    }

    //replace double _ for column name from namespaces
    columnName = columnName.replace(/(.)__([^c].)/gi, '$1_$2');
    
    //if prefix will be added then we may have an issue with field name length
    //so, for now i just don't add prefix to column name

    //const prefix = `${(columnName.indexOf('__c') > 0 ? migratedCustomTablePrefix : migratedTablePrefix)}`
    //return prefix + '_' + (columnName.indexOf('__c') > 0 ? columnName : columnName + '__c');


    return (columnName.indexOf('__c') > 0 ? columnName : columnName + '__c');
}

const LONG_TEXT_LENGTH = 131072;
const TEXT_LENGTH = 255;

function getFieldLength(column) {

    const { columnName, dataType, length} = column;

    if (columnName === 'sfid') {
        return 18
    }

    if (dataType === 'text' || (dataType === 'varchar' && (useLongTextAreaFieldType || length >= 255))) {
        return LONG_TEXT_LENGTH;
    }

    return TEXT_LENGTH;
}

async function getMetadataJson(schemaName, tableName) {
    const columns = await getColumns(schemaName, tableName);
    const objectName = getSalesforceCustomObjectName(tableName)
    return {
        fullName : objectName,
        label : objectName,
        pluralLabel : objectName,
        deploymentStatus: 'Deployed',
        sharingModel: 'ReadWrite',
        nameField: {
            type: 'AutoNumber',
            label: 'Auto Number'
        },
        fields : columns.map(column => {
            const sfField = {
                fullName : convertColumnNameToSFformat(column.columnName),
                label : column.columnName,
                type : (column.dataType === 'text' || useLongTextAreaFieldType) ? 'LongTextArea' : 'Text',
                length : getFieldLength(column),
                externalId : column.columnName === 'sfid',
                unique : column.columnName === 'sfid',
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
    return `${PERMISION_SET_NAME_TEMPLATE} ${objectName}`.replaceAll(' ', '_').replaceAll('__', '_');
}

module.exports = {
    convertColumnNameToSFformat,
    getExternalIdFieldName,
    getMetadataJson,
    getPermissionSetJson,
    getPermissionSetName
}