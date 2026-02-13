//! Shared test utilities for MynaDB integration tests.
//!
//! Provides a `TestServer` struct with extended helpers for InfluxQL queries,
//! schema/table inspection, and response parsing.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

/// Global counter for unique database names across all test files.
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test harness for running MynaDB integration tests.
///
/// Starts an isolated MynaDB server with a temporary data directory.
/// The server is automatically killed when the `TestServer` is dropped.
pub struct TestServer {
    process: Option<Child>,
    pub port: u16,
    pub data_dir: PathBuf,
    _temp_dir: Option<tempfile::TempDir>,
}

impl TestServer {
    /// Start a new MynaDB test server with default config.
    pub fn start() -> Self {
        Self::start_with_config_str("")
    }

    /// Start a new MynaDB test server with additional TOML config appended.
    pub fn start_with_config_str(extra_config: &str) -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let port = find_available_port();
        let config_path = temp_dir.path().join("config.toml");
        let storage_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&storage_dir).expect("Failed to create storage dir");

        let storage_dir_str = storage_dir.to_str().unwrap().replace('\\', "/");

        // Build base config, skipping sections that are overridden by extra_config
        let mut base = format!(
            "[server]\n\
            http_port = {port}\n\
            mysql_enabled = false\n\
            flight_enabled = false\n\
            \n\
            [storage]\n\
            data_dir = \"{data_dir}\"\n\
            \n\
            [storage.object_store]\n\
            backend = \"local\"\n\
            data_dir = \"{data_dir}\"\n\
            \n\
            [cache]\n\
            flush_interval_secs = 1\n\
            max_size_mb = 256\n\
            \n\
            [wal]\n\
            enabled = false\n\
            \n\
            [compaction]\n\
            min_files = 100\n\
            \n\
            [logging]\n\
            level = \"warn\"\n",
            port = port,
            data_dir = storage_dir_str,
        );

        // Only add default [retention] if extra_config doesn't define it
        if !extra_config.contains("[retention]") {
            base.push_str(
                "\n[retention]\n\
                enabled = false\n",
            );
        }

        let config_content = format!("{}\n{}\n", base, extra_config);
        std::fs::write(&config_path, &config_content).expect("Failed to write config");

        let binary_path = find_binary_path();
        let process = Command::new(&binary_path)
            .args(["-c", config_path.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start MynaDB from {:?}", binary_path));

        let server = TestServer {
            process: Some(process),
            port,
            data_dir: storage_dir,
            _temp_dir: Some(temp_dir),
        };

        server.wait_for_ready();
        server
    }

    /// Start a server with authentication enabled.
    ///
    /// Generates an Argon2 password hash for the given password using the mynadb binary,
    /// then configures [[auth.users]] with the hashed password.
    pub fn start_with_auth(user: &str, pass: &str) -> Self {
        // Generate Argon2 hash using the binary
        let binary_path = find_binary_path();
        let output = std::process::Command::new(&binary_path)
            .args(["hash-password", pass])
            .output()
            .expect("Failed to run mynadb hash-password");
        let hash = String::from_utf8(output.stdout)
            .expect("Invalid UTF-8 from hash-password")
            .trim()
            .to_string();
        assert!(
            hash.starts_with("$argon2id$"),
            "Expected Argon2 hash, got: {}",
            hash
        );

        let auth_config = format!(
            "[auth]\n\
            enabled = true\n\
            \n\
            [[auth.users]]\n\
            username = \"{user}\"\n\
            password_hash = \"{hash}\"\n\
            roles = [\"admin\"]\n",
            user = user,
            hash = hash,
        );
        Self::start_with_config_str(&auth_config)
    }

    /// Start a server with retention enabled.
    pub fn start_with_retention(default_days: u32, interval_secs: u64) -> Self {
        let retention_config = format!(
            "[retention]\n\
            enabled = true\n\
            default_days = {days}\n\
            interval_secs = {interval}\n",
            days = default_days,
            interval = interval_secs,
        );
        Self::start_with_config_str(&retention_config)
    }

    fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/health", self.port);
        for _ in 0..60 {
            if let Ok(resp) = ureq::get(&url).call() {
                if resp.status() == 200 {
                    thread::sleep(Duration::from_millis(100));
                    return;
                }
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!(
            "Server failed to start within 30 seconds on port {}",
            self.port
        );
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Generate a unique database name for test isolation.
    pub fn unique_db(&self) -> String {
        format!("testdb_{}", DB_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    // =========================================================================
    // Write / Flush / Compact
    // =========================================================================

    /// Write data using InfluxDB line protocol.
    pub fn write(&self, db: &str, data: &str) -> Result<(), String> {
        let url = format!("{}/write/{}", self.base_url(), db);
        match ureq::post(&url).send_string(data) {
            Ok(resp) => {
                let status = resp.status();
                if status != 200 && status != 204 {
                    return Err(format!("Write failed with status: {}", status));
                }
                Ok(())
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Write failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("Write failed: {}", e)),
        }
    }

    /// Write data with authentication credentials.
    pub fn write_with_auth(
        &self,
        db: &str,
        data: &str,
        user: &str,
        pass: &str,
    ) -> Result<(), String> {
        let url = format!("{}/write/{}", self.base_url(), db);
        match ureq::post(&url)
            .set("Authorization", &basic_auth_header(user, pass))
            .send_string(data)
        {
            Ok(resp) => {
                let status = resp.status();
                if status != 200 && status != 204 {
                    return Err(format!("Write failed with status: {}", status));
                }
                Ok(())
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Write failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("Write failed: {}", e)),
        }
    }

    /// Write gzip-compressed data.
    pub fn write_gzip(&self, db: &str, data: &[u8]) -> Result<(), String> {
        let url = format!("{}/write/{}", self.base_url(), db);
        match ureq::post(&url)
            .set("Content-Encoding", "gzip")
            .send_bytes(data)
        {
            Ok(resp) => {
                let status = resp.status();
                if status != 200 && status != 204 {
                    return Err(format!("Write failed with status: {}", status));
                }
                Ok(())
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Write gzip failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("Write gzip failed: {}", e)),
        }
    }

    /// Flush data to Parquet.
    pub fn flush(&self) -> Result<(), String> {
        let url = format!("{}/flush", self.base_url());
        match ureq::post(&url).call() {
            Ok(resp) => {
                if resp.status() != 200 {
                    return Err(format!("Flush failed with status: {}", resp.status()));
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                return Err(format!("Flush failed with status {}: {}", code, body));
            }
            Err(e) => return Err(format!("Flush failed: {}", e)),
        }
        thread::sleep(Duration::from_secs(2));
        Ok(())
    }

    /// Trigger compaction.
    pub fn compact(&self, db: &str) -> Result<(), String> {
        let url = format!("{}/compact/{}", self.base_url(), db);
        match ureq::post(&url).call() {
            Ok(_) => {}
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                return Err(format!("Compact failed with status {}: {}", code, body));
            }
            Err(e) => return Err(format!("Compact failed: {}", e)),
        }
        thread::sleep(Duration::from_secs(2));
        Ok(())
    }

    // =========================================================================
    // SQL Query
    // =========================================================================

    /// Execute SQL query and return results as JSON (with retry).
    pub fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/query/{}", self.base_url(), db);
        for attempt in 0..5 {
            let resp = ureq::post(&url).send_string(sql);
            match resp {
                Ok(resp) => {
                    let body = resp
                        .into_string()
                        .map_err(|e| format!("Failed to read response: {}", e))?;
                    return serde_json::from_str(&body)
                        .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body));
                }
                Err(ureq::Error::Status(code, resp)) => {
                    let body = resp.into_string().unwrap_or_default();
                    if code == 400 && body.contains("not found") && attempt < 4 {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    return Err(format!("Query failed with status {}: {}", code, body));
                }
                Err(e) => return Err(format!("Query failed: {}", e)),
            }
        }
        Err("Query failed after retries".to_string())
    }

    /// Query and extract a single numeric value.
    pub fn query_scalar(&self, db: &str, sql: &str) -> Result<f64, String> {
        let result = self.query(db, sql)?;
        extract_scalar(&result)
    }

    /// Query and extract count as integer.
    pub fn query_count(&self, db: &str, sql: &str) -> Result<i64, String> {
        let result = self.query(db, sql)?;
        extract_count(&result)
    }

    /// Query and extract all rows as Vec of HashMaps.
    pub fn query_rows(
        &self,
        db: &str,
        sql: &str,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
        let result = self.query(db, sql)?;
        extract_rows(&result)
    }

    /// Execute SQL query with authentication.
    pub fn query_with_auth(
        &self,
        db: &str,
        sql: &str,
        user: &str,
        pass: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!("{}/query/{}", self.base_url(), db);
        match ureq::post(&url)
            .set("Authorization", &basic_auth_header(user, pass))
            .send_string(sql)
        {
            Ok(resp) => {
                let body = resp
                    .into_string()
                    .map_err(|e| format!("Failed to read response: {}", e))?;
                serde_json::from_str(&body)
                    .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Query failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("Query failed: {}", e)),
        }
    }

    // =========================================================================
    // InfluxQL Query
    // =========================================================================

    /// Execute InfluxQL query via GET /query?db=&q=&epoch=ms
    pub fn query_influxql(
        &self,
        db: &str,
        influxql: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!(
            "{}/query?db={}&q={}&epoch=ms",
            self.base_url(),
            db,
            urlencoded(influxql),
        );
        for attempt in 0..5 {
            match ureq::get(&url).call() {
                Ok(resp) => {
                    let body = resp
                        .into_string()
                        .map_err(|e| format!("Failed to read response: {}", e))?;
                    return serde_json::from_str(&body)
                        .map_err(|e| format!("Failed to parse InfluxQL JSON: {} (body: {})", e, body));
                }
                Err(ureq::Error::Status(code, resp)) => {
                    let body = resp.into_string().unwrap_or_default();
                    if code == 400 && body.contains("not found") && attempt < 4 {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    return Err(format!(
                        "InfluxQL query failed with status {}: {}",
                        code, body
                    ));
                }
                Err(e) => return Err(format!("InfluxQL query failed: {}", e)),
            }
        }
        Err("InfluxQL query failed after retries".to_string())
    }

    /// Execute InfluxQL query via POST /query?db=&epoch=ms (query in body).
    pub fn query_influxql_post(
        &self,
        db: &str,
        influxql: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!("{}/query?db={}&epoch=ms", self.base_url(), db);
        let body = format!("q={}", urlencoded(influxql));
        match ureq::post(&url)
            .set("Content-Type", "application/x-www-form-urlencoded")
            .send_string(&body)
        {
            Ok(resp) => {
                let body = resp
                    .into_string()
                    .map_err(|e| format!("Failed to read response: {}", e))?;
                serde_json::from_str(&body)
                    .map_err(|e| format!("Failed to parse InfluxQL JSON: {} (body: {})", e, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!(
                    "InfluxQL POST query failed with status {}: {}",
                    code, body
                ))
            }
            Err(e) => Err(format!("InfluxQL POST query failed: {}", e)),
        }
    }

    /// Execute InfluxQL query with auth via GET.
    pub fn query_influxql_with_auth(
        &self,
        db: &str,
        influxql: &str,
        user: &str,
        pass: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!(
            "{}/query?db={}&q={}&epoch=ms",
            self.base_url(),
            db,
            urlencoded(influxql),
        );
        match ureq::get(&url)
            .set("Authorization", &basic_auth_header(user, pass))
            .call()
        {
            Ok(resp) => {
                let body = resp
                    .into_string()
                    .map_err(|e| format!("Failed to read response: {}", e))?;
                serde_json::from_str(&body)
                    .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("InfluxQL auth query failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("InfluxQL auth query failed: {}", e)),
        }
    }

    // =========================================================================
    // Metadata / Schema endpoints
    // =========================================================================

    /// GET /tables/:db — list tables in database.
    pub fn tables(&self, db: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/tables/{}", self.base_url(), db);
        http_get_json(&url)
    }

    /// GET /schema/:db/:schema/:table — get table schema.
    pub fn schema_info(&self, db: &str, table: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/schema/{}/default/{}", self.base_url(), db, table);
        http_get_json(&url)
    }

    /// GET /explain/:db?sql=... — explain query plan.
    pub fn explain(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!(
            "{}/explain/{}?sql={}",
            self.base_url(),
            db,
            urlencoded(sql),
        );
        http_get_json(&url)
    }

    /// GET /analyze/:db?sql=... — analyze query.
    pub fn analyze(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!(
            "{}/analyze/{}?sql={}",
            self.base_url(),
            db,
            urlencoded(sql),
        );
        http_get_json(&url)
    }

    // =========================================================================
    // Monitoring endpoints
    // =========================================================================

    /// GET /health
    pub fn health(&self) -> Result<(u16, String), String> {
        let url = format!("{}/health", self.base_url());
        http_get_status_and_body(&url)
    }

    /// GET /stats
    pub fn stats(&self) -> Result<(u16, String), String> {
        let url = format!("{}/stats", self.base_url());
        http_get_status_and_body(&url)
    }

    /// GET /memory
    pub fn memory(&self) -> Result<(u16, String), String> {
        let url = format!("{}/memory", self.base_url());
        http_get_status_and_body(&url)
    }

    // =========================================================================
    // Cache endpoints
    // =========================================================================

    /// GET /cache/stats
    pub fn cache_stats(&self) -> Result<serde_json::Value, String> {
        let url = format!("{}/cache/stats", self.base_url());
        http_get_json(&url)
    }

    /// POST /cache/clear
    pub fn cache_clear(&self) -> Result<serde_json::Value, String> {
        let url = format!("{}/cache/clear", self.base_url());
        http_post_json(&url, "")
    }

    // =========================================================================
    // Partition endpoints
    // =========================================================================

    /// GET /partitions/:catalog/:schema/:table
    pub fn partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!(
            "{}/partitions/{}/{}/{}",
            self.base_url(),
            catalog,
            schema,
            table,
        );
        http_get_json(&url)
    }

    // =========================================================================
    // Retention endpoints
    // =========================================================================

    /// POST /retention/:catalog/:schema/:table with JSON body.
    pub fn retention(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        body: &serde_json::Value,
    ) -> Result<(u16, String), String> {
        let url = format!(
            "{}/retention/{}/{}/{}",
            self.base_url(),
            catalog,
            schema,
            table,
        );
        match ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_string(&body.to_string())
        {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.into_string().unwrap_or_default();
                Ok((status, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Ok((code, body))
            }
            Err(e) => Err(format!("Retention request failed: {}", e)),
        }
    }

    // =========================================================================
    // Raw HTTP helpers for auth testing
    // =========================================================================

    /// Make a raw GET request and return (status, body).
    pub fn raw_get(&self, path: &str) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url(), path);
        http_get_status_and_body(&url)
    }

    /// Make a raw GET request with auth and return (status, body).
    pub fn raw_get_with_auth(
        &self,
        path: &str,
        user: &str,
        pass: &str,
    ) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url(), path);
        match ureq::get(&url)
            .set("Authorization", &basic_auth_header(user, pass))
            .call()
        {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.into_string().unwrap_or_default();
                Ok((status, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Ok((code, body))
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    /// Make a raw POST request and return (status, body).
    pub fn raw_post(&self, path: &str, body: &str) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url(), path);
        match ureq::post(&url).send_string(body) {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.into_string().unwrap_or_default();
                Ok((status, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Ok((code, body))
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    /// Make a raw POST request with auth and return (status, body).
    pub fn raw_post_with_auth(
        &self,
        path: &str,
        body: &str,
        user: &str,
        pass: &str,
    ) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url(), path);
        match ureq::post(&url)
            .set("Authorization", &basic_auth_header(user, pass))
            .send_string(body)
        {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.into_string().unwrap_or_default();
                Ok((status, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Ok((code, body))
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(ref mut process) = self.process {
            let _ = process.kill();
            let _ = process.wait();
        }
    }
}

// =============================================================================
// Extraction helpers
// =============================================================================

/// Extract a scalar numeric value from SQL query result.
/// Handles format: [{"column_name": value}]
pub fn extract_scalar(result: &serde_json::Value) -> Result<f64, String> {
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                for (_, val) in obj.iter() {
                    if let Some(f) = val.as_f64() {
                        return Ok(f);
                    }
                    if let Some(i) = val.as_i64() {
                        return Ok(i as f64);
                    }
                }
            }
        }
    }
    Err(format!("Failed to extract scalar from: {:?}", result))
}

/// Extract count value from SQL query result.
pub fn extract_count(result: &serde_json::Value) -> Result<i64, String> {
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                for (_, val) in obj.iter() {
                    if let Some(i) = val.as_i64() {
                        return Ok(i);
                    }
                    if let Some(f) = val.as_f64() {
                        return Ok(f as i64);
                    }
                }
            }
        }
    }
    Err(format!("Failed to extract count from: {:?}", result))
}

/// Extract all rows from SQL query result as Vec of HashMaps.
pub fn extract_rows(
    result: &serde_json::Value,
) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    if let Some(arr) = result.as_array() {
        let mut rows = Vec::new();
        for item in arr {
            if let Some(obj) = item.as_object() {
                let mut row = HashMap::new();
                for (k, v) in obj.iter() {
                    row.insert(k.clone(), v.clone());
                }
                rows.push(row);
            }
        }
        return Ok(rows);
    }
    Err(format!("Could not extract rows from: {:?}", result))
}

// =============================================================================
// InfluxQL response helpers
// =============================================================================

/// Extract the series array from an InfluxQL response.
/// Response format: {"results": [{"series": [{"name": ..., "columns": [...], "values": [[...]]}]}]}
pub fn influxql_series(result: &serde_json::Value) -> Vec<&serde_json::Value> {
    result
        .get("results")
        .and_then(|r| r.as_array())
        .into_iter()
        .flatten()
        .filter_map(|stmt| stmt.get("series"))
        .filter_map(|s| s.as_array())
        .flatten()
        .collect()
}

/// Extract values from the first series in an InfluxQL response.
pub fn influxql_values(result: &serde_json::Value) -> Vec<Vec<serde_json::Value>> {
    let series = influxql_series(result);
    if let Some(first) = series.first() {
        if let Some(values) = first.get("values").and_then(|v| v.as_array()) {
            return values
                .iter()
                .map(|row| {
                    row.as_array()
                        .map(|a| a.clone())
                        .unwrap_or_default()
                })
                .collect();
        }
    }
    Vec::new()
}

/// Extract column names from the first series.
pub fn influxql_columns(result: &serde_json::Value) -> Vec<String> {
    let series = influxql_series(result);
    if let Some(first) = series.first() {
        if let Some(cols) = first.get("columns").and_then(|c| c.as_array()) {
            return cols
                .iter()
                .filter_map(|c| c.as_str().map(String::from))
                .collect();
        }
    }
    Vec::new()
}

/// Get the measurement name from the first series.
pub fn influxql_measurement(result: &serde_json::Value) -> Option<String> {
    let series = influxql_series(result);
    series
        .first()
        .and_then(|s| s.get("name"))
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Get the error message from an InfluxQL response, if any.
pub fn influxql_error(result: &serde_json::Value) -> Option<String> {
    result
        .get("results")
        .and_then(|r| r.as_array())
        .and_then(|arr| arr.first())
        .and_then(|stmt| stmt.get("error"))
        .and_then(|e| e.as_str())
        .map(String::from)
}

/// Count total rows across all series in an InfluxQL response.
pub fn influxql_row_count(result: &serde_json::Value) -> usize {
    influxql_series(result)
        .iter()
        .filter_map(|s| s.get("values"))
        .filter_map(|v| v.as_array())
        .map(|a| a.len())
        .sum()
}

/// Assert the number of rows in an InfluxQL response.
pub fn assert_influxql_row_count(result: &serde_json::Value, expected: usize) {
    let actual = influxql_row_count(result);
    assert_eq!(
        actual, expected,
        "Expected {} InfluxQL rows, got {}. Response: {:?}",
        expected, actual, result
    );
}

/// Extract column values as f64 from InfluxQL result by column index.
pub fn influxql_column_f64(result: &serde_json::Value, col_idx: usize) -> Vec<Option<f64>> {
    influxql_values(result)
        .iter()
        .map(|row| row.get(col_idx).and_then(|v| v.as_f64()))
        .collect()
}

// =============================================================================
// Utility functions
// =============================================================================

pub fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

pub fn find_binary_path() -> PathBuf {
    let release = std::env::current_dir()
        .unwrap()
        .join("target/release/mynadb");
    if release.exists() {
        release
    } else {
        std::env::current_dir()
            .unwrap()
            .join("target/debug/mynadb")
    }
}

fn basic_auth_header(user: &str, pass: &str) -> String {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", user, pass));
    format!("Basic {}", encoded)
}

fn urlencoded(s: &str) -> String {
    s.replace(' ', "%20")
        .replace('+', "%2B")
        .replace('\'', "%27")
        .replace('(', "%28")
        .replace(')', "%29")
        .replace('*', "%2A")
        .replace('>', "%3E")
        .replace('<', "%3C")
        .replace('=', "%3D")
        .replace('"', "%22")
        .replace(',', "%2C")
        .replace(';', "%3B")
}

fn http_get_json(url: &str) -> Result<serde_json::Value, String> {
    match ureq::get(url).call() {
        Ok(resp) => {
            let body = resp
                .into_string()
                .map_err(|e| format!("Failed to read response: {}", e))?;
            serde_json::from_str(&body)
                .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body))
        }
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            Err(format!("GET failed with status {}: {}", code, body))
        }
        Err(e) => Err(format!("GET failed: {}", e)),
    }
}

fn http_get_status_and_body(url: &str) -> Result<(u16, String), String> {
    match ureq::get(url).call() {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.into_string().unwrap_or_default();
            Ok((status, body))
        }
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            Ok((code, body))
        }
        Err(e) => Err(format!("GET failed: {}", e)),
    }
}

fn http_post_json(url: &str, body: &str) -> Result<serde_json::Value, String> {
    match ureq::post(url).send_string(body) {
        Ok(resp) => {
            let body = resp
                .into_string()
                .map_err(|e| format!("Failed to read response: {}", e))?;
            serde_json::from_str(&body)
                .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body))
        }
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            Err(format!("POST failed with status {}: {}", code, body))
        }
        Err(e) => Err(format!("POST failed: {}", e)),
    }
}
