const zlib = require('zlib');

const {
    shouldCompressTable,
    isCompressedField,
    maybeCompressFieldValue
} = require('../services/migrationCompression');

describe('services/migrationCompression.js', () => {
    it('should enable compression only for emailmessage table', () => {
        expect(shouldCompressTable('emailmessage')).toBe(true);
        expect(shouldCompressTable('EmailMessage')).toBe(true);
        expect(shouldCompressTable('case')).toBe(false);
    });

    it('should mark only emailmessage.htmlbody as compressed field', () => {
        expect(isCompressedField('emailmessage', 'htmlbody')).toBe(true);
        expect(isCompressedField('emailmessage', 'subject')).toBe(false);
        expect(isCompressedField('case', 'htmlbody')).toBe(false);
    });

    it('should gzip only emailmessage.htmlbody values', () => {
        const source = '<html><body>Hello</body></html>';
        const compressed = maybeCompressFieldValue('emailmessage', 'htmlbody', source);

        expect(compressed).not.toBe(source);
        const restored = zlib.gunzipSync(Buffer.from(compressed, 'base64')).toString('utf8');
        expect(restored).toBe(source);

        expect(maybeCompressFieldValue('emailmessage', 'subject', source)).toBe(source);
        expect(maybeCompressFieldValue('case', 'htmlbody', source)).toBe(source);
    });
});
