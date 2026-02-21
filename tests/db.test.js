const { Pool } = require('pg');

// Mock the pg Pool
jest.mock('pg', () => {
    const mClient = {
        query: jest.fn(),
        release: jest.fn(),
    };
    const mPool = {
        connect: jest.fn(() => mClient),
        query: jest.fn(),
        end: jest.fn(),
    };
    return { Pool: jest.fn(() => mPool) };
});

// Mock config
jest.mock('../config/default', () => ({
    clientDbUrl: 'postgres://user:pass@localhost:5432/testdb',
    pcSchema: 'test_schema',
    hcSchema: 'heroku_schema'
}));

const db = require('../services/db');

describe('services/db.js - Commit 6338299: Column Length Lookup Fix', () => {
    let mockPool;
    let mockClient;

    beforeEach(() => {
        jest.clearAllMocks();
        mockPool = new Pool();
        mockClient = mockPool.connect();
    });

    describe('getColumns() - Array.find() fix', () => {
        it('should correctly find and update column length from materialized view', async () => {
            // Mock getTableMetadata response
            mockPool.query
                // First call: getTableMetadata
                .mockResolvedValueOnce({
                    rows: [
                        { columnName: 'description', dataType: 'text', length: null },
                        { columnName: 'status', dataType: 'varchar', length: 50 },
                        { columnName: 'comments', dataType: 'text', length: null }
                    ]
                })
                // Second call: materialized view query
                .mockResolvedValueOnce({
                    rows: [
                        { column_name: 'description', data_type: 'text', column_size: 73 },
                        { column_name: 'comments', data_type: 'text', column_size: 150 }
                    ]
                });

            const result = await db.getColumns('test_schema', 'case');

            // Verify columns are updated correctly
            expect(result).toHaveLength(3);
            expect(result[0]).toEqual({ columnName: 'description', dataType: 'text', length: 73 });
            expect(result[1]).toEqual({ columnName: 'status', dataType: 'varchar', length: 50 });
            expect(result[2]).toEqual({ columnName: 'comments', dataType: 'text', length: 150 });
        });

        it('should handle column not found in materialized view', async () => {
            mockPool.query
                .mockResolvedValueOnce({
                    rows: [
                        { columnName: 'title', dataType: 'text', length: null }
                    ]
                })
                .mockResolvedValueOnce({
                    rows: [] // No results from materialized view
                });

            const result = await db.getColumns('test_schema', 'article');

            // Should keep null length since no data from materialized view
            expect(result[0]).toEqual({ columnName: 'title', dataType: 'text', length: null });
        });

        it('should not query materialized view if all columns have length', async () => {
            mockPool.query.mockResolvedValueOnce({
                rows: [
                    { columnName: 'status', dataType: 'varchar', length: 50 },
                    { columnName: 'code', dataType: 'varchar', length: 10 }
                ]
            });

            const result = await db.getColumns('test_schema', 'lookup');

            // Should only call query once (getTableMetadata)
            expect(mockPool.query).toHaveBeenCalledTimes(1);
            expect(result).toHaveLength(2);
        });

        it('should handle column_size of 0 from materialized view', async () => {
            mockPool.query
                .mockResolvedValueOnce({
                    rows: [
                        { columnName: 'empty_field', dataType: 'text', length: null }
                    ]
                })
                .mockResolvedValueOnce({
                    rows: [
                        { column_name: 'empty_field', data_type: 'text', column_size: 0 }
                    ]
                });

            const result = await db.getColumns('test_schema', 'test_table');

            // Should NOT update length when column_size is 0 (falsy)
            expect(result[0]).toEqual({ columnName: 'empty_field', dataType: 'text', length: null });
        });

        it('should only update columns that exist in both arrays', async () => {
            mockPool.query
                .mockResolvedValueOnce({
                    rows: [
                        { columnName: 'field1', dataType: 'text', length: null },
                        { columnName: 'field2', dataType: 'text', length: null }
                    ]
                })
                .mockResolvedValueOnce({
                    rows: [
                        { column_name: 'field1', data_type: 'text', column_size: 100 },
                        { column_name: 'field999', data_type: 'text', column_size: 200 } // Doesn't exist!
                    ]
                });

            const result = await db.getColumns('test_schema', 'test_table');

            expect(result[0]).toEqual({ columnName: 'field1', dataType: 'text', length: 100 });
            expect(result[1]).toEqual({ columnName: 'field2', dataType: 'text', length: null });
        });
    });
});

describe('services/db.js - Commit e7879be: Refresh Materialized View Feature', () => {
    let mockPool;

    beforeEach(() => {
        jest.clearAllMocks();
        mockPool = new Pool();
    });

    describe('dropMaterializedView()', () => {
        it('should execute DROP MATERIALIZED VIEW query', async () => {
            mockPool.query.mockResolvedValueOnce({ rows: [] });

            await db.dropMaterializedView('test_schema');

            expect(mockPool.query).toHaveBeenCalled();
            const callArg = mockPool.query.mock.calls[0][0];
            expect(callArg).toContain('DROP MATERIALIZED VIEW IF EXISTS test_schema.pcma_tables_info_mv');
        });

        it('should use default schema when none provided', async () => {
            mockPool.query.mockResolvedValueOnce({ rows: [] });

            await db.dropMaterializedView();

            expect(mockPool.query).toHaveBeenCalled();
            const callArg = mockPool.query.mock.calls[0][0];
            expect(callArg).toContain('test_schema.pcma_tables_info_mv');
        });

        it('should throw error if DROP fails', async () => {
            const error = new Error('Permission denied');
            mockPool.query.mockRejectedValueOnce(error);

            await expect(db.dropMaterializedView('test_schema')).rejects.toThrow('Permission denied');
        });

        it('should handle IF EXISTS correctly (no error if view does not exist)', async () => {
            mockPool.query.mockResolvedValueOnce({ rows: [] });

            await expect(db.dropMaterializedView('test_schema')).resolves.not.toThrow();
        });
    });

    describe('buildMaterializedViewWithTablesInfo() - with refresh capability', () => {
        it('should not create view if it already exists', async () => {
            // Mock isMaterializedViewExisting to return true
            mockPool.query.mockResolvedValueOnce({ rows: [{ exists: true }] });

            await db.buildMaterializedViewWithTablesInfo('test_schema');

            // Should only call once (for exists check), not for creation
            expect(mockPool.query).toHaveBeenCalledTimes(1);
        });

        it('should create view if it does not exist', async () => {
            // Mock isMaterializedViewExisting to return false
            mockPool.query
                .mockResolvedValueOnce({ rows: [{ exists: false }] })
                .mockResolvedValueOnce({ rows: [] }); // CREATE MATERIALIZED VIEW

            await db.buildMaterializedViewWithTablesInfo('test_schema');

            expect(mockPool.query).toHaveBeenCalledTimes(2);
            const secondCall = mockPool.query.mock.calls[1][0];
            expect(secondCall).toContain('CREATE MATERIALIZED VIEW');
        });
    });

    describe('isMaterializedViewExisting()', () => {
        it('should return true when view exists', async () => {
            mockPool.query.mockResolvedValueOnce({ rows: [{ exists: true }] });

            const result = await db.isMaterializedViewExisting('test_schema');

            expect(result).toBe(true);
        });

        it('should return false when view does not exist', async () => {
            mockPool.query.mockResolvedValueOnce({ rows: [{ exists: false }] });

            const result = await db.isMaterializedViewExisting('test_schema');

            expect(result).toBe(false);
        });
    });

    describe('buildMaterializedViewWithTablesInfo() - error propagation fix', () => {
        it('should throw error when view creation fails', async () => {
            const error = new Error('Database error: permission denied');
            mockPool.query
                .mockResolvedValueOnce({ rows: [{ exists: false }] })  // View doesn't exist
                .mockRejectedValueOnce(error);  // CREATE fails

            await expect(db.buildMaterializedViewWithTablesInfo('test_schema'))
                .rejects.toThrow('Database error: permission denied');
        });

        it('should not swallow errors in catch block', async () => {
            const error = new Error('Syntax error in SQL');
            mockPool.query
                .mockResolvedValueOnce({ rows: [{ exists: false }] })
                .mockRejectedValueOnce(error);

            // Verify error is propagated, not swallowed
            await expect(db.buildMaterializedViewWithTablesInfo('test_schema'))
                .rejects.toThrow(error);
        });
    });

    describe('queryStream() - connection leak fix', () => {
        it('should pass client to QueryStream for proper cleanup', async () => {
            const mockClient = {
                query: jest.fn(),
                release: jest.fn()
            };
            mockPool.connect.mockResolvedValueOnce(mockClient);

            const stream = await db.queryStream('SELECT * FROM test', 1000);

            // Verify stream has client property
            expect(stream.client).toBe(mockClient);
        });

        it('should release client when stream is destroyed', async () => {
            const mockClient = {
                query: jest.fn().mockReturnValue({
                    read: jest.fn((size, cb) => cb(null, [])),
                    close: jest.fn((cb) => cb())
                }),
                release: jest.fn()
            };
            mockPool.connect.mockResolvedValueOnce(mockClient);

            const stream = await db.queryStream('SELECT * FROM test', 1000);
            
            // Destroy the stream
            await new Promise((resolve) => {
                stream.on('close', resolve);
                stream.destroy();
            });

            // Verify client was released
            expect(mockClient.release).toHaveBeenCalledTimes(1);
        });

        it('should release client even when error occurs', async () => {
            const mockClient = {
                query: jest.fn().mockReturnValue({
                    read: jest.fn((size, cb) => cb(new Error('Read error'), null)),
                    close: jest.fn((cb) => cb())
                }),
                release: jest.fn()
            };
            mockPool.connect.mockResolvedValueOnce(mockClient);

            const stream = await db.queryStream('SELECT * FROM test', 1000);
            
            // Destroy stream with error
            await new Promise((resolve) => {
                stream.on('close', resolve);
                stream.destroy(new Error('Stream error'));
            });

            // Verify client was still released despite error
            expect(mockClient.release).toHaveBeenCalledTimes(1);
        });
    });
});

