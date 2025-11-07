
const { 
    migratedTablePrefix,
    migratedCustomTablePrefix
} = require('./../config/default');

const { hashWithBase64 } = require('./../services/utils');

const { getColumns } = require('./db');

const version = 1;

function replaceDoubleUnderscore(name) {
    return name.replace(/(.)__(.)/gi, '$1_$2').replace(/_c$/i, '__c');
}

function getPrefixBasedOnTableName(tableName) {
    return tableName.endsWith('__c') ? migratedCustomTablePrefix : migratedTablePrefix; //different prefix for custom and standard tables
}

function getSalesforceCustomObjectName(tableName) {
    const hasedTableName = hashWithBase64(tableName); //20 chars hash to avoid issues with length and duplicates
    const prefix = getPrefixBasedOnTableName(tableName);
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
const MAPPED_FIELD_NAME = 'field';

function getFieldMetadata(column) {

    const { columnName, dataType, length} = column;

    if (columnName === 'sfid') {
        return { length : 18, type : TEXT_TYPE }
    }

    if (dataType === 'bool' || dataType === 'boolean') {
        return { length : 5, type : TEXT_TYPE };
    }

    if (dataType === 'date') {
        return { length : 10, type : TEXT_TYPE };
    }

    if (dataType === 'timestamp') {
        return { length : 25, type : TEXT_TYPE };
    }

    if (length > 255) {
        return { length, type : LONG_TEXT_TYPE };
    }

    return { length : length || 1, type : TEXT_TYPE };
}


const SALESFORCE_LONG_TEXTAREA_MAX_LENGTH =  1638000;

async function getMetadataJson(schemaName, tableName) {

    //get columns for table with information about max column length, it may take time for large tables
    const columns = await getColumns(schemaName, tableName)

    const prefix = getPrefixBasedOnTableName(tableName);
    const newLabel = `${prefix}_${tableName}`;
    const result = {
        fullName : getSalesforceCustomObjectName(tableName), //convert sf object name to new object name to prevent issues with length and duplicates
        description : makeDescriptionJson(version, tableName),
        label : newLabel.slice(0, 40), //max label length is 40
        pluralLabel : newLabel.slice(0, 40), //max label length is 40
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
                description : makeDescriptionJson(version, column.columnName),
                label : column.columnName.slice(0, 40), //max label length is 40
                type,
                length,
                externalId : isSfId,
                unique : isSfId
            };

            if (sfField.type === 'LongTextArea') {
                sfField.visibleLines = 3;
            }

            return sfField;
        })
    }

    //Salesforce has limit of 1,638,000 chars for all text fields in one object
    let totalLengthOfTextFields = 0;
    result.fields.filter(f => f.type === LONG_TEXT_TYPE).forEach(f => {
        totalLengthOfTextFields += (f.length || 0);
    })

    if (totalLengthOfTextFields >= SALESFORCE_LONG_TEXTAREA_MAX_LENGTH) {
        throw new Error(`Total length of LongTextArea fields ${totalLengthOfTextFields} for "${tableName}" exceeds maximum allowed ${SALESFORCE_LONG_TEXTAREA_MAX_LENGTH}`);
    }

    return result;
}

const PERMISION_SET_NAME_TEMPLATE = 'PCMA Permission Set For';

function getPermissionSetJson(objectMetadata) {
    return {
        label : `${PERMISION_SET_NAME_TEMPLATE} ${objectMetadata.label}`,
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

function makeDescriptionJson(version, apiName) {
    return JSON.stringify({
        version,
        apiName : apiName === 'sfid' ? 'Id' : apiName
    });
}

module.exports = {
    convertColumnNameToSFformat,
    getExternalIdFieldName,
    getMetadataJson,
    getPermissionSetJson,
    getPermissionSetName,
    getMappedFieldName
}