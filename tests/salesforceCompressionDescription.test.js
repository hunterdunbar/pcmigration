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

    it('should set isCompressed=1 for emailmessage.htmlbody', async () => {
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: 100 },
            { columnName: 'subject', dataType: 'text', length: 50 }
        ]);

        const metadata = await getMetadataJson('cache', 'emailmessage');
        const htmlBodyDescription = getFieldDescriptionByLabel(metadata, 'htmlbody');
        const subjectDescription = getFieldDescriptionByLabel(metadata, 'subject');

        expect(htmlBodyDescription.isCompressed).toBe(1);
        expect(subjectDescription.isCompressed).toBeUndefined();
    });

    it('should set isCompressed=0 for non-emailmessage htmlbody', async () => {
        mockGetColumns.mockResolvedValue([
            { columnName: 'htmlbody', dataType: 'text', length: 100 }
        ]);

        const metadata = await getMetadataJson('cache', 'case');
        const htmlBodyDescription = getFieldDescriptionByLabel(metadata, 'htmlbody');

        expect(htmlBodyDescription.isCompressed).toBe(0);
    });
});
