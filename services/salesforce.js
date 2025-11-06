
const { 
    migratedCustomTablePrefix, 
    migratedTablePrefix,
    useLongTextAreaFieldType
} = require('./../config/default');

const { hash20base64 } = require('./../services/utils');

const { getColumns } = require('./db');

function replaceDoubleUnderscore(name) {
    return name.replace(/(.)__(.)/gi, '$1_$2').replace(/_c$/i, '__c');
}

function getSalesforceCustomObjectName(tableName) {
    const hasedTableName = hash20base64(tableName); //20 chars hash to avoid issues with length and duplicates
    const prefix = tableName.endsWith('__c') ? migratedCustomTablePrefix : migratedTablePrefix; //different prefix for custom and standard tables
    return `${prefix}_${hasedTableName}__c`;
}

function getExternalIdFieldName() {
    return 'field_id__c';
}

//deprecated
function convertColumnNameToSFformat(columnName) {
    if (columnName === 'sfid') {
        return getExternalIdFieldName();
    }

    //replace double _ for column name from namespaces
    columnName = replaceDoubleUnderscore(columnName);
    
    //if prefix will be added then we may have an issue with field name length
    //so, for now i just don't add prefix to column name

    //const prefix = `${(columnName.indexOf('__c') > 0 ? migratedCustomTablePrefix : migratedTablePrefix)}`
    //return prefix + '_' + (columnName.indexOf('__c') > 0 ? columnName : columnName + '__c');


    return (columnName.endsWith('__c') ? columnName : columnName + '__c');
}

const TEXT_TYPE = 'Text';
const LONG_TEXT_TYPE = 'LongTextArea';
const BOOLEAN_TYPE = 'Checkbox';
const MAPPED_FIELD_NAME = 'field';

function getFieldMetadata(column) {

    const { columnName, dataType, length} = column;

    if (columnName === 'sfid') {
        return { length : 18, type : TEXT_TYPE }
    }

    if (dataType === 'bool' || dataType === 'boolean') {
        return { length : null, type : BOOLEAN_TYPE };
    }

    if (dataType === 'text' || (dataType === 'varchar' && (useLongTextAreaFieldType || length > 255))) {
        return { length, type : LONG_TEXT_TYPE };
    }

    return { length : length ? length : 10, type : TEXT_TYPE };
}

async function getMetadataJson(schemaName, tableName) {
    const columns = await getColumns(schemaName, tableName);
    const objectName = getSalesforceCustomObjectName(tableName);

    return {
        fullName : objectName,
        description : tableName,
        label : tableName.slice(0, 40), //max label length is 40
        pluralLabel : tableName.slice(0, 40), //max label length is 40
        deploymentStatus: 'Deployed',
        sharingModel: 'ReadWrite',
        nameField: {
            type: 'AutoNumber',
            label: 'Auto Number'
        },
        fields : columns.map((column, i) => {
            const { type, length } = getFieldMetadata(column);
            const isSfId = column.columnName === 'sfid';
            const sfField = {
                fullName : getMappedFieldName(column.columnName, i),
                description : column.columnName,
                label : column.columnName.slice(0, 40), //max label length is 40
                type,
                externalId : isSfId,
                unique : isSfId
            };

            if (type === BOOLEAN_TYPE) {
                sfField.defaultValue = 'false';
            }

            if (length) {
                sfField.length = length;
            }

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

function getMappedFieldName(name, index) {
    if (name === 'sfid') {
        return getExternalIdFieldName(); //keep sfid as is for external id field
    }
    return `${MAPPED_FIELD_NAME}_${index}__c`; //mapped field names to avoid issues with length or duplicates
}

module.exports = {
    convertColumnNameToSFformat,
    getExternalIdFieldName,
    getMetadataJson,
    getPermissionSetJson,
    getPermissionSetName,
    getMappedFieldName
}