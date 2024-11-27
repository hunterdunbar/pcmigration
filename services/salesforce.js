
const MIGRATED_SF_STANDARD_OBJECT_PREFIX = process.env.MIGRATED_STANDRD_OBJECT_PREFIX || 'migrated';
const MIGRATED_SF_CUSTOM_OBJECT_PREFIX = process.env.MIGRATED_CUSTOM_OBJECT_PREFIX || 'migrated_custom';



function getSalesforceCustomObjectName(tableName) {
    return `${(tableName.indexOf('__c') > 0 ? MIGRATED_SF_CUSTOM_OBJECT_PREFIX : MIGRATED_SF_STANDARD_OBJECT_PREFIX)}_${tableName}__c`
}

function getExternalIdFieldName() {
    return MIGRATED_SF_STANDARD_OBJECT_PREFIX + '_id__c';
}

function convertColumnNameToSFformat(columnName) {
    if (columnName === 'sfid') {
        return getExternalIdFieldName();
    }

    //replace double _ for column name from namespaces
    columnName = columnName.replace(/(.)__([^c].)/gi, '$1_$2');
    const prefix = `${(columnName.indexOf('__c') > 0 ? MIGRATED_SF_CUSTOM_OBJECT_PREFIX : MIGRATED_SF_STANDARD_OBJECT_PREFIX)}`
    return prefix + '_' + (columnName.indexOf('__c') > 0 ? columnName : columnName + '__c');
}


function getMetadataJson(tableName, tableMetada) {
    const objectName = getSalesforceCustomObjectName(tableName)
    return {
        fullName : objectName,
        label : objectName,
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
    return `${PERMISION_SET_NAME_TEMPLATE} ${objectName}`.replaceAll(' ', '_').replaceAll('__', '_');
}

module.exports = {
    convertColumnNameToSFformat,
    getExternalIdFieldName,
    getMetadataJson,
    getPermissionSetJson,
    getPermissionSetName
}