const { gzipSync } = require('zlib');

const COMPRESSED_TABLE_NAME = 'emailmessage';
const COMPRESSED_COLUMN_NAME = 'htmlbody';
const COMPRESSED_FIELD_MAX_PLAIN_LENGTH = 131072;

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

// Decides whether compression should be enabled for a field based on max source length.
// Returns true only for emailmessage.htmlbody and only when maxLength > 131072.
// If maxLength is missing/invalid, returns true as a safe fallback.
function shouldCompressFieldByLength(tableName, columnName, maxLength) {
    if (!isCompressedField(tableName, columnName)) {
        return false;
    }

    // Missing max length means we cannot safely skip compression.
    if (maxLength === null || maxLength === undefined || maxLength === '') {
        return true;
    }

    const normalizedMaxLength = Number(maxLength);
    if (!Number.isFinite(normalizedMaxLength)) {
        // If max length is unknown, keep compression enabled as a safe fallback.
        return true;
    }

    // Compress only when source max length exceeds Salesforce LongTextArea limit.
    return normalizedMaxLength > COMPRESSED_FIELD_MAX_PLAIN_LENGTH;
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
    COMPRESSED_COLUMN_NAME,
    COMPRESSED_FIELD_MAX_PLAIN_LENGTH,
    shouldCompressTable,
    isCompressedField,
    shouldCompressFieldByLength,
    maybeCompressFieldValue
};
