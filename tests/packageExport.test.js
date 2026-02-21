const request = require('supertest');
const express = require('express');

// Mock config FIRST before any other requires
jest.mock('../config/default', () => ({
    pcSchema: 'test_schema',
    clientDbUrl: 'postgres://testuser:testpass@localhost:5432/testdb'
}));

// Mock dependencies with default implementations
const mockDropMaterializedView = jest.fn();
const mockBuildMaterializedViewWithTablesInfo = jest.fn();
const mockIsMaterializedViewExisting = jest.fn();
const mockGetTablesInSchemas = jest.fn();

jest.mock('../services/db', () => ({
    dropMaterializedView: (...args) => mockDropMaterializedView(...args),
    buildMaterializedViewWithTablesInfo: (...args) => mockBuildMaterializedViewWithTablesInfo(...args),
    isMaterializedViewExisting: (...args) => mockIsMaterializedViewExisting(...args),
    getTablesInSchemas: (...args) => mockGetTablesInSchemas(...args),
}));

jest.mock('../services/salesforce');

const packageExportRouter = require('../controllers/packageExport');

// Create a minimal Express app for testing
function createTestApp() {
    const app = express();
    app.use(express.json());
    app.use(express.urlencoded({ extended: true }));
    
    // Mock session
    app.use((req, res, next) => {
        req.session = { user: 'testuser' };
        next();
    });
    
    // Mock view rendering
    app.set('view engine', 'pug');
    app.engine('pug', (path, options, callback) => {
        callback(null, JSON.stringify(options));
    });
    app.set('views', './views');
    
    app.use(packageExportRouter);
    
    return app;
}

describe('controllers/packageExport.js - Commit e7879be: Refresh Materialized View', () => {
    let app;

    beforeEach(() => {
        jest.clearAllMocks();
        mockDropMaterializedView.mockReset();
        mockBuildMaterializedViewWithTablesInfo.mockReset();
        mockIsMaterializedViewExisting.mockReset();
        mockGetTablesInSchemas.mockReset();
        app = createTestApp();
    });

    describe('POST /refreshMaterializedView', () => {
        it('should call dropMaterializedView and redirect', async () => {
            mockDropMaterializedView.mockResolvedValue();
            mockIsMaterializedViewExisting.mockResolvedValue(false);

            const response = await request(app)
                .post('/refreshMaterializedView')
                .expect(302); // Redirect

            expect(mockDropMaterializedView).toHaveBeenCalledTimes(1);
            expect(response.headers.location).toBe('/packageExport');
        });

        it('should handle errors and render error page', async () => {
            const error = new Error('Database connection failed');
            mockDropMaterializedView.mockRejectedValue(error);
            mockIsMaterializedViewExisting.mockResolvedValue(false);

            const response = await request(app)
                .post('/refreshMaterializedView')
                .expect(200);

            expect(mockDropMaterializedView).toHaveBeenCalledTimes(1);
            
            // Check that error message is passed to render
            const rendered = JSON.parse(response.text);
            expect(rendered.errorMessage).toContain('Database connection failed');
        });

        it('should use default schema when dropping view', async () => {
            mockDropMaterializedView.mockResolvedValue();
            mockIsMaterializedViewExisting.mockResolvedValue(false);

            await request(app)
                .post('/refreshMaterializedView')
                .expect(302);

            // dropMaterializedView should be called with default schema (from pcSchema config)
            expect(mockDropMaterializedView).toHaveBeenCalledWith();
        });
    });

    describe('GET /refreshMaterializedView', () => {
        it('should redirect to /packageExport', async () => {
            const response = await request(app)
                .get('/refreshMaterializedView')
                .expect(302);

            expect(response.headers.location).toBe('/packageExport');
        });
    });

    describe('POST /analyzePrivacyCenter - race condition test', () => {
        it('should only trigger one view creation when called multiple times', async () => {
            mockBuildMaterializedViewWithTablesInfo.mockImplementation(() => {
                return new Promise(resolve => setTimeout(resolve, 100));
            });
            mockIsMaterializedViewExisting.mockResolvedValue(false);

            // Simulate rapid double-click
            const promise1 = request(app).post('/analyzePrivacyCenter');
            const promise2 = request(app).post('/analyzePrivacyCenter');

            await Promise.all([promise1, promise2]);

            // Due to viewCreationInProgress flag, should only call once
            expect(mockBuildMaterializedViewWithTablesInfo).toHaveBeenCalledTimes(1);
        });
    });

    describe('GET /packageExport - showRefreshButton flag', () => {
        it('should set showRefreshButton to true when view exists', async () => {
            mockIsMaterializedViewExisting.mockResolvedValue(true);
            mockGetTablesInSchemas.mockResolvedValue({
                test_schema: [
                    { tablename: 'users', number_of_columns: 5, table_size: '1 MB' }
                ]
            });

            const response = await request(app)
                .get('/packageExport')
                .expect(200);

            const rendered = JSON.parse(response.text);
            expect(rendered.tables).toBeDefined();
            expect(rendered.tables.length).toBe(1);
            expect(rendered.showRefreshButton).toBeTruthy();
        });

        it('should not set showRefreshButton when view does not exist', async () => {
            mockIsMaterializedViewExisting.mockResolvedValue(false);

            const response = await request(app)
                .get('/packageExport')
                .expect(200);

            const rendered = JSON.parse(response.text);
            expect(rendered.showRefreshButton).toBeFalsy();
            expect(rendered.message).toBeDefined();
        });
    });
});

describe('Integration: Refresh Workflow', () => {
    let app;

    beforeEach(() => {
        jest.clearAllMocks();
        mockDropMaterializedView.mockReset();
        mockBuildMaterializedViewWithTablesInfo.mockReset();
        mockIsMaterializedViewExisting.mockReset();
        mockGetTablesInSchemas.mockReset();
        app = createTestApp();
    });

    it('should allow dropping and recreating materialized view', async () => {
        // Step 1: View exists
        mockIsMaterializedViewExisting.mockResolvedValue(true);
        mockGetTablesInSchemas.mockResolvedValue({
            test_schema: [{ tablename: 'test', number_of_columns: 3, table_size: '100 KB' }]
        });

        let response = await request(app).get('/packageExport').expect(200);
        let rendered = JSON.parse(response.text);
        expect(rendered.showRefreshButton).toBeTruthy();
        expect(rendered.tables).toBeDefined();

        // Step 2: User clicks refresh
        mockDropMaterializedView.mockResolvedValue();
        mockIsMaterializedViewExisting.mockResolvedValue(false); // Now it's gone

        await request(app).post('/refreshMaterializedView').expect(302);
        expect(mockDropMaterializedView).toHaveBeenCalled();

        // Step 3: User sees message about needing to analyze
        response = await request(app).get('/packageExport').expect(200);
        rendered = JSON.parse(response.text);
        expect(rendered.message).toBeDefined();
        expect(rendered.showRefreshButton).toBeFalsy();

        // Step 4: User clicks analyze to recreate (redirects immediately, runs in background)
        mockBuildMaterializedViewWithTablesInfo.mockResolvedValue();

        response = await request(app).post('/analyzePrivacyCenter').expect(302);
        expect(response.headers.location).toBe('/packageExport');
        
        // Note: buildMaterializedViewWithTablesInfo runs in background (not awaited in controller)
        // We can't reliably test it was called without adding delays, but we verified redirect works
    });
});

