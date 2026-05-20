const mockGetColumns = jest.fn();

jest.mock('../services/db', () => ({
    getColumns: (...args) => mockGetColumns(...args)
}));

const { getMetadataJson } = require('../services/salesforce');

function getFieldDescriptionByLabel(metadata, label) {
    const field = metadata.fields.find(f => f.label === label);
    return field ? JSON.parse(field.description) : null;
}

describe('services/salesforce.js compression description', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        mockGetColumns.mockReset();
    });

    it('should set isCompressed=0 for emailmessage.htmlbody when max length does not exceed limit', async () => {
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: 100 },
            { columnName: 'subject', dataType: 'text', length: 50 }
        ]);

        const metadata = await getMetadataJson('cache', 'emailmessage');
        const htmlBodyDescription = getFieldDescriptionByLabel(metadata, 'htmlbody');
        const subjectDescription = getFieldDescriptionByLabel(metadata, 'subject');

        expect(htmlBodyDescription.isCompressed).toBe(0);
        expect(subjectDescription.isCompressed).toBeUndefined();
    });

    it('should set isCompressed=1 for emailmessage.htmlbody when max length exceeds limit', async () => {
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: 200000 }
        ]);

        const metadata = await getMetadataJson('cache', 'emailmessage');
        const htmlBodyDescription = getFieldDescriptionByLabel(metadata, 'htmlbody');

        expect(htmlBodyDescription.isCompressed).toBe(1);
    });

    it('should set isCompressed=0 for non-emailmessage htmlbody', async () => {
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: 100 }
        ]);

        const metadata = await getMetadataJson('cache', 'case');
        const htmlBodyDescription = getFieldDescriptionByLabel(metadata, 'htmlbody');

        expect(htmlBodyDescription.isCompressed).toBe(0);
    });

    it('should sum LongTextArea lengths numerically when length is a string (bigint from pg)', async () => {
        // Reproduces the bug where pg bigint returns string and "+= f.length" string-concats,
        // producing a huge bogus total like "0131072131072131072..." that triggered a false
        // "exceeds maximum allowed 1638000" error.
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: '131072' },
            { columnName: 'body',     dataType: 'text', length: '131072' },
            { columnName: 'subject',  dataType: 'text', length: '131072' }
        ]);

        // Should not throw; numeric sum is 393216 < 1638000.
        const metadata = await getMetadataJson('cache', 'emailmessage');
        expect(metadata.fields).toHaveLength(3);
    });

    it('should cap LongTextArea length to Salesforce max field size', async () => {
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: 1048250 }
        ]);

        const metadata = await getMetadataJson('cache', 'emailmessage');
        const htmlBodyField = metadata.fields.find(f => f.label === 'htmlbody');

        expect(htmlBodyField.type).toBe('LongTextArea');
        expect(htmlBodyField.length).toBe(131072);
    });
});
