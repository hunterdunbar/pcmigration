

function convertColumnNameToSFformat(columnName) {
    if (columnName === 'sfid') {
        return 'original_sfid__c';
    }

    //replace double _ for column name from namespaces
    columnName = columnName.replace(/(.)__([^c].)/gi, '$1_$2');

    return columnName.indexOf('__c') > 0 ? columnName : columnName + '__c';
}

module.exports = {
    convertColumnNameToSFformat
}