use crate::query::query_engine::QueryEngine;
use crate::storage::cache::DataCache;
use msql_srv::{
    Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, MysqlIntermediary, MysqlShim,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::{debug, error, info};

/// MySQL protocol service for MynaDB
pub struct MysqlService {
    query_engine: Arc<QueryEngine>,
    runtime_handle: Handle,
    data_dir: String,
    data_cache: Arc<DataCache>,
}

impl MysqlService {
    pub fn new(
        query_engine: Arc<QueryEngine>,
        runtime_handle: Handle,
        data_dir: String,
        data_cache: Arc<DataCache>,
    ) -> Self {
        Self {
            query_engine,
            runtime_handle,
            data_dir,
            data_cache,
        }
    }

    /// Run the MySQL server on the given address
    /// This blocks and should be run in a dedicated thread
    pub fn run(&self, addr: &str) -> io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!("MySQL server listening on {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let query_engine = self.query_engine.clone();
                    let runtime_handle = self.runtime_handle.clone();
                    let data_dir = self.data_dir.clone();
                    let data_cache = self.data_cache.clone();
                    std::thread::spawn(move || {
                        let session =
                            MysqlSession::new(query_engine, runtime_handle, data_dir, data_cache);
                        if let Err(e) = MysqlIntermediary::run_on_tcp(session, stream) {
                            error!("MySQL session error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("MySQL connection error: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// Per-connection MySQL session state
struct MysqlSession {
    query_engine: Arc<QueryEngine>,
    runtime_handle: Handle,
    data_dir: String,
    data_cache: Arc<DataCache>,
    current_db: Option<String>,
    prepared_statements: HashMap<u32, PreparedStatement>,
    next_stmt_id: u32,
}

#[derive(Clone)]
struct PreparedStatement {
    sql: String,
    #[allow(dead_code)]
    param_count: usize,
}

impl MysqlSession {
    fn new(
        query_engine: Arc<QueryEngine>,
        runtime_handle: Handle,
        data_dir: String,
        data_cache: Arc<DataCache>,
    ) -> Self {
        Self {
            query_engine,
            runtime_handle,
            data_dir,
            data_cache,
            current_db: None,
            prepared_statements: HashMap::new(),
            next_stmt_id: 1,
        }
    }

    /// Execute a query and write results
    fn execute_query<W: Read + Write>(
        &self,
        sql: &str,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let db = match &self.current_db {
            Some(db) => db.clone(),
            None => {
                return results.error(
                    ErrorKind::ER_NO_DB_ERROR,
                    b"No database selected. Use: USE <database>;",
                );
            }
        };

        // Run async query using stored runtime handle
        let query_result = self.runtime_handle.block_on(async {
            // Ensure database is registered
            self.query_engine.ensure_database_registered(&db).await?;
            // Execute query
            self.query_engine.execute_query_with_db(sql, &db).await
        });

        match query_result {
            Ok(batches) => {
                if batches.is_empty() {
                    // No results - return empty result set
                    let writer = results.start(&[])?;
                    return writer.finish();
                }

                // Build column definitions from first batch schema
                let schema = batches[0].schema();
                let columns: Vec<Column> = schema
                    .fields()
                    .iter()
                    .map(|field| Column {
                        table: String::new(),
                        column: field.name().clone(),
                        coltype: arrow_type_to_mysql(field.data_type()),
                        colflags: if field.is_nullable() {
                            ColumnFlags::empty()
                        } else {
                            ColumnFlags::NOT_NULL_FLAG
                        },
                    })
                    .collect();

                let mut writer = results.start(&columns)?;

                // Write rows from all batches
                for batch in &batches {
                    let num_rows = batch.num_rows();
                    let num_cols = batch.num_columns();

                    for row_idx in 0..num_rows {
                        for col_idx in 0..num_cols {
                            let value = arrow_value_to_mysql(batch.column(col_idx), row_idx);
                            match value {
                                MysqlValue::Null => writer.write_col(None::<&str>)?,
                                MysqlValue::Int(v) => writer.write_col(v)?,
                                MysqlValue::UInt(v) => writer.write_col(v)?,
                                MysqlValue::Float(v) => writer.write_col(v)?,
                                MysqlValue::String(v) => writer.write_col(v)?,
                                MysqlValue::Bool(v) => writer.write_col(v as i8)?,
                            }
                        }
                        writer.end_row()?;
                    }
                }

                writer.finish()
            }
            Err(e) => {
                error!("Query error: {}", e);
                results.error(ErrorKind::ER_UNKNOWN_ERROR, e.as_bytes())
            }
        }
    }
}

/// Internal enum for MySQL value representation
enum MysqlValue {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bool(bool),
}

/// Convert Arrow DataType to MySQL ColumnType
fn arrow_type_to_mysql(dt: &datafusion::arrow::datatypes::DataType) -> ColumnType {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int8 => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int16 => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::UInt8 => ColumnType::MYSQL_TYPE_TINY,
        DataType::UInt16 => ColumnType::MYSQL_TYPE_SHORT,
        DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ColumnType::MYSQL_TYPE_VAR_STRING
        }
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Date32 | DataType::Date64 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Binary | DataType::LargeBinary => ColumnType::MYSQL_TYPE_BLOB,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING, // Fallback to string
    }
}

/// Convert Arrow array value to MySQL value
fn arrow_value_to_mysql(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    index: usize,
) -> MysqlValue {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType;

    if array.is_null(index) {
        return MysqlValue::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            MysqlValue::Bool(arr.value(index))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            MysqlValue::Int(arr.value(index) as i64)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            MysqlValue::Int(arr.value(index) as i64)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            MysqlValue::Int(arr.value(index) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            MysqlValue::Int(arr.value(index))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            MysqlValue::UInt(arr.value(index) as u64)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            MysqlValue::UInt(arr.value(index) as u64)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            MysqlValue::UInt(arr.value(index) as u64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            MysqlValue::UInt(arr.value(index))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            MysqlValue::Float(arr.value(index) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            MysqlValue::Float(arr.value(index))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            MysqlValue::String(arr.value(index).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            MysqlValue::String(arr.value(index).to_string())
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            MysqlValue::String(arr.value(index).to_string())
        }
        DataType::Timestamp(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let ts = arr.value(index);
            // Convert nanoseconds to datetime string
            let secs = ts / 1_000_000_000;
            let nsecs = (ts % 1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                MysqlValue::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            } else {
                MysqlValue::String(ts.to_string())
            }
        }
        _ => {
            // Fallback: convert to string representation
            MysqlValue::String(format!("{:?}", array.data_type()))
        }
    }
}

impl<W: Read + Write> MysqlShim<W> for MysqlSession {
    type Error = io::Error;

    /// Called when client sends USE database
    fn on_init(&mut self, database: &str, writer: InitWriter<W>) -> io::Result<()> {
        info!("MySQL: USE {}", database);

        // Check if database exists by trying to discover it
        let _db_exists = self.runtime_handle.block_on(async {
            self.query_engine
                .ensure_database_registered(database)
                .await
                .is_ok()
        });

        // Always allow setting the database - it might be created later
        self.current_db = Some(database.to_string());
        writer.ok()
    }

    /// Called when client sends a query
    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        debug!("MySQL query: {}", query);

        let query_upper = query.trim().to_uppercase();

        // Handle USE command (some clients send it as query, not init)
        if query_upper.starts_with("USE ") {
            let db = query.trim()[4..].trim().trim_end_matches(';').to_string();
            self.current_db = Some(db.clone());
            info!("MySQL: USE {} (via query)", db);
            let writer = results.start(&[])?;
            return writer.finish();
        }

        // Handle SHOW DATABASES
        if query_upper.starts_with("SHOW DATABASES") {
            return self.handle_show_databases(results);
        }

        // Handle SHOW TABLES
        if query_upper.starts_with("SHOW TABLES") {
            return self.handle_show_tables(results);
        }

        // Handle SELECT DATABASE()
        if query_upper.contains("SELECT DATABASE()") {
            return self.handle_select_database(results);
        }

        // Handle SET commands (ignore them)
        if query_upper.starts_with("SET ") {
            let writer = results.start(&[])?;
            return writer.finish();
        }

        // Execute regular query
        self.execute_query(query, results)
    }

    /// Called when client sends PREPARE
    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        debug!("MySQL prepare: {}", query);

        // Count parameter placeholders (?)
        let param_count = query.matches('?').count();

        // Store prepared statement
        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        self.prepared_statements.insert(
            stmt_id,
            PreparedStatement {
                sql: query.to_string(),
                param_count,
            },
        );

        // Build parameter columns (all as strings for simplicity)
        let params: Vec<Column> = (0..param_count)
            .map(|i| Column {
                table: String::new(),
                column: format!("param_{}", i),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();

        // We don't know result columns until execution, so return empty
        info.reply(stmt_id, &params, &[])
    }

    /// Called when client sends EXECUTE
    fn on_execute(
        &mut self,
        stmt_id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        debug!("MySQL execute: stmt_id={}", stmt_id);

        let stmt = match self.prepared_statements.get(&stmt_id) {
            Some(s) => s.clone(),
            None => {
                return results.error(
                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                    b"Unknown prepared statement",
                );
            }
        };

        // Substitute parameters into SQL
        let mut sql = stmt.sql.clone();
        for param in params {
            use msql_srv::ValueInner;
            let value_inner = param.value.into_inner();
            let value_str = match value_inner {
                ValueInner::NULL => "NULL".to_string(),
                ValueInner::Bytes(b) => {
                    let s = String::from_utf8_lossy(b);
                    format!("'{}'", s.replace('\'', "''"))
                }
                ValueInner::Int(i) => i.to_string(),
                ValueInner::UInt(u) => u.to_string(),
                ValueInner::Double(d) => d.to_string(),
                ValueInner::Date(b) | ValueInner::Datetime(b) => {
                    // Parse binary date format or fallback to hex string
                    let s = String::from_utf8_lossy(b);
                    format!("'{}'", s)
                }
                ValueInner::Time(b) => {
                    let s = String::from_utf8_lossy(b);
                    format!("'{}'", s)
                }
            };

            // Replace first ? with the value
            if let Some(pos) = sql.find('?') {
                sql.replace_range(pos..pos + 1, &value_str);
            }
        }

        self.execute_query(&sql, results)
    }

    /// Called when client closes a prepared statement
    fn on_close(&mut self, stmt_id: u32) {
        debug!("MySQL close: stmt_id={}", stmt_id);
        self.prepared_statements.remove(&stmt_id);
    }
}

impl MysqlSession {
    /// Handle SHOW DATABASES command
    /// Returns databases from both filesystem and in-memory cache
    fn handle_show_databases<W: Read + Write>(
        &self,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // List databases from data directory
        let data_dir = self.data_dir.clone();
        let data_cache = self.data_cache.clone();

        let databases = self.runtime_handle.block_on(async {
            let mut databases = Vec::new();

            // Get databases from filesystem
            if let Ok(entries) = std::fs::read_dir(&data_dir) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            if !name.starts_with('.') {
                                databases.push(name.to_string());
                            }
                        }
                    }
                }
            }

            // Get databases from cache (memtable) - may not be flushed yet
            let cached_dbs = data_cache.get_cached_catalogs();
            for db in cached_dbs {
                if !databases.contains(&db) {
                    databases.push(db);
                }
            }

            databases.sort();
            databases
        });

        let columns = vec![Column {
            table: String::new(),
            column: "Database".to_string(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::NOT_NULL_FLAG,
        }];

        let mut writer = results.start(&columns)?;
        for db in databases {
            writer.write_col(&db)?;
            writer.end_row()?;
        }
        writer.finish()
    }

    /// Handle SHOW TABLES command
    /// Returns tables from both filesystem and in-memory cache
    fn handle_show_tables<W: Read + Write>(&self, results: QueryResultWriter<W>) -> io::Result<()> {
        let db = match &self.current_db {
            Some(db) => db.clone(),
            None => {
                return results.error(ErrorKind::ER_NO_DB_ERROR, b"No database selected");
            }
        };

        let data_cache = self.data_cache.clone();

        let tables = self.runtime_handle.block_on(async {
            // Get schemas and tables from filesystem (via query engine)
            let mut all_tables = Vec::new();
            if let Ok(schemas) = self.query_engine.list_schemas(&db).await {
                for schema in schemas {
                    if let Ok(tables) = self.query_engine.list_tables_in_schema(&db, &schema).await
                    {
                        for table in tables {
                            all_tables.push(format!("{}.{}", schema, table));
                        }
                    }
                }
            }

            // Get schemas and tables from cache (memtable) - may not be flushed yet
            let cached_schemas = data_cache.get_cached_schemas(&db);
            for schema in cached_schemas {
                let cached_tables = data_cache.get_cached_tables(&db, &schema);
                for table in cached_tables {
                    let full_name = format!("{}.{}", schema, table);
                    if !all_tables.contains(&full_name) {
                        all_tables.push(full_name);
                    }
                }
            }

            all_tables.sort();
            all_tables
        });

        let columns = vec![Column {
            table: String::new(),
            column: format!("Tables_in_{}", db),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::NOT_NULL_FLAG,
        }];

        let mut writer = results.start(&columns)?;
        for table in tables {
            writer.write_col(&table)?;
            writer.end_row()?;
        }
        writer.finish()
    }

    /// Handle SELECT DATABASE() command
    fn handle_select_database<W: Read + Write>(
        &self,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let columns = vec![Column {
            table: String::new(),
            column: "DATABASE()".to_string(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        }];

        let mut writer = results.start(&columns)?;
        match &self.current_db {
            Some(db) => writer.write_col(db)?,
            None => writer.write_col(None::<&str>)?,
        }
        writer.end_row()?;
        writer.finish()
    }
}
