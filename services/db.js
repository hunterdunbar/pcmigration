const { Pool } = require('pg');
const { parse } = require('pg-connection-string');
const Cursor = require('pg-cursor');
const { Readable, Writable } = require('stream');


if (!process.env.CLIENT_DATABASE_URL) {
    throw new Error('CLIENT_DATABASE_URL is not defined');
}

const dbConfig = parse(process.env.CLIENT_DATABASE_URL)

const pool = new Pool({
    ...dbConfig,
    ssl : {
        rejectUnauthorized : false,
    },
    idleTimeoutMillis: 0,
    connectionTimeoutMillis: 0,
})

async function query(sql, params) {
    const client = await pool.connect();
    try {
        return client.query(sql, params);
    } finally {
        client.release();
    }
    
}

async function getSchemas() {
    const result = await query('SELECT schema_name FROM information_schema.schemata where schema_owner = $1', [ dbConfig.user ])
    return result?.rows?.map(r => r.schema_name);
}


async function getTablesInSchemas(schemas = []) {
    if (!schemas?.length) {
        throw new Error('No one schema is selected');
    }
    const result = await query(`select schemaname, tablename, pg_size_pretty( pg_total_relation_size(schemaname || '.' || tablename)) table_size, \
        (select COUNT(*) from information_schema.columns where table_name = tablename) number_of_columns from pg_catalog.pg_tables \
        where not(starts_with(tablename, '_')) and tableowner = $1 and schemaname in (${schemas.map((v, i) => '$' + (2 + i)).join(',')}) order by tablename`, 
        [ dbConfig.user, ...schemas ]);

    return result?.rows?.reduce((groupedData, row) => {
        if (!groupedData[row.schemaname]) {
            groupedData[row.schemaname] = []
        }

        groupedData[row.schemaname].push(row);

        return groupedData;
    }, {});
}

function getTableMetadata(tableNameWithSchema) {
    return query(`select column_name "columnName", udt_name "dataType", character_maximum_length length \
        from information_schema.columns where column_name != 'id' and not(starts_with(column_name, '__')) and table_schema = $1 and table_name = $2`, tableNameWithSchema.split('.'));
}

async function getTablesInfo(tableNamesWithSchema = []) {
    if (!tableNamesWithSchema?.length) {
        throw new Error('No one table is selected');
    }

    const result = await Promise.all(
        tableNamesWithSchema.map(async tableName => {
            const tableMetada = await getTableMetadata(tableName);
            const countOfRows = await query(`select count(*) from ${tableName}`);
            const tableSize = await query(`SELECT pg_total_relation_size('${tableName}') table_size`);
            return { 
                [tableName] : { 
                    countOfRows : countOfRows?.rows?.[0]?.count, 
                    tableSize : tableSize?.rows?.[0]?.table_size,
                    tableMetada : tableMetada?.rows
                } 
            }
        })
    );
    if (Array.isArray(result)) {
        return result.reduce((groupedData, res) => {
            groupedData = { ...groupedData, ...res }
            return groupedData
        }, {})
    }
    
    return []
}

async function queryCursor(sql, params, config = {}, callback) {
    const client = await pool.connect();
    const cursor = await client.query(new Cursor(sql, params));
    
    try {
        let rows = []
        do {
            rows = await cursor.read(config.chunkSize || 10000);
            await callback(rows)
        } while (rows?.length !== 0) 
                
    } catch (e) {
        console.error('[ERROR]: queryCursor: ', e);
        await cursor.close()
        throw e;
    } finally {
        client.release();
    }
}

class QueryStream extends Readable {

    cursor;

    constructor(cursor, chunkSize) {
        super({ highWaterMark: chunkSize, objectMode : true });
        this.cursor = cursor;
    }

    _read(size) {
        this.cursor.read(size, (err, rows) => {
            if (err) {
                this.destroy(err)
            } else {
                this.push(rows.length < size ? null : rows);
            }
        });
    }

    _destroy(err, callback) {
        if (err) {
            console.error('[ERROR]: QueryStream: ', err);
        }
        this.cursor.close(callback)
    }

}

const hcSchema = process.env.HC_SCHEMA

class HerokuSchemaWriter extends Writable {
    
    dbClient;
    tableName;

    constructor(dbClient, tableName) {
        super({ objectMode : true })

        this.dbClient = dbClient;
        this.tableName = tableName;
    }

    _write(data, encoding, done) {
        if (data?.length) {
            const columns = Object.keys(data?.[0])?.join(', ');
            const values = data.map(row => Object.values(row).map(val => val ? `'${val}'` : 'NULL').join(',')).map(val => `(${val})`)
            const query = `insert into ${hcSchema}.${this.tableName}(${columns}) values ${values.join(',')} ON CONFLICT (sfid__c) DO NOTHING`;
            this.dbClient.query(query, (err, res) => {
                if (err) {
                    this.destroy(err)
                } else {
                    console.debug(res);
                    done()
                }
            });
            
        }
    }

    _destroy(err, callback) {
        if (err) {
            console.error('[ERROR]: QueryStream: ', err);
        }
        this.dbClient.release();

        callback();
    }
}


async function queryStream(sql, chunkSize) {
    const client = await pool.connect();
    const cursor = client.query(new Cursor(sql));
    return new QueryStream(cursor, chunkSize);
}

async function hcWriterStream(tableName) {
    const client = await pool.connect();
    return new HerokuSchemaWriter(client, tableName);
}

async function isTableExisting(tableName, schemaName = 'public') {
    const result = await query('SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema = $1 AND table_name = $2);', [ schemaName, tableName ]);

    return result?.rows?.[0]?.exists || false;
}


module.exports = {
    getSchemas,
    getTablesInSchemas,
    getTablesInfo,
    getTableMetadata,
    queryCursor,
    queryStream,
    hcWriterStream,
    query,
    isTableExisting
}