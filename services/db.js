const { Pool } = require('pg');
const { parse } = require('pg-connection-string');

const {
    clientDbUrl
} = require('./../config/default');


if (!clientDbUrl) {
    throw new Error('CLIENT_DATABASE_URL is not defined');
}

const dbConfig = parse(clientDbUrl)

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

function getTableMetadata(schemaName, tableSchema) {
    return query(`select column_name "columnName", udt_name "dataType", character_maximum_length length \
        from information_schema.columns where column_name != 'id' and not(starts_with(column_name, '__')) \
        and table_schema = $1 and table_name = $2`, [ schemaName, tableSchema ]);
}

module.exports = {
    getTablesInSchemas,
    getTableMetadata,
    query
}