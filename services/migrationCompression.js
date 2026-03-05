const { gzipSync } = require('zlib');

const COMPRESSED_TABLE_NAME = 'emailmessage';
const COMPRESSED_COLUMN_NAME = 'htmlbody';

function normalizeName(value) {
    return String(value || '').toLowerCase();
}

function shouldCompressTable(tableName) {
    return normalizeName(tableName) === COMPRESSED_TABLE_NAME;
}

function isCompressedField(tableName, columnName) {
    return shouldCompressTable(tableName)
        && normalizeName(columnName) === COMPRESSED_COLUMN_NAME;
}

// Used by the migration worker (index.js, compressed path) before INSERT:
// convert emailmessage.htmlbody values to gzip+base64 so the payload fits Salesforce LongTextArea limits.
function maybeCompressFieldValue(tableName, columnName, value) {
    if (!isCompressedField(tableName, columnName)) {
        return value;
    }

    if (value === null || value === undefined) {
        return null;
    }

    if (Buffer.isBuffer(value)) {
        return gzipSync(value).toString('base64');
    }

    return gzipSync(Buffer.from(String(value), 'utf8')).toString('base64');
}

module.exports = {
    shouldCompressTable,
    isCompressedField,
    maybeCompressFieldValue
};
