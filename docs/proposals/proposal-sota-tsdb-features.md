# Proposal: Building a State-of-the-Art Time Series Database

**Status:** Draft
**Author:** System Architecture Team
**Date:** 2026-02-06
**Target Version:** 2.0

---

## Executive Summary

MynaDB has established a strong foundation with Raft-based clustering, multi-protocol support (HTTP/Arrow Flight/MySQL), LSM-style compaction, and warm standby replication. This proposal outlines the strategic features required to position MynaDB as a **state-of-the-art time series database** competitive with InfluxDB, TimescaleDB, VictoriaMetrics, and ClickHouse.

**Core Thesis:** To achieve SOTA status, MynaDB must excel in four dimensions:
1. **Storage Efficiency** - Downsampling, adaptive compression, data tiering
2. **Query Performance** - Streaming queries, distributed execution, advanced caching
3. **Enterprise Readiness** - Security (TLS, encryption at rest), RBAC, backup/restore
4. **Ecosystem Integration** - Grafana, Prometheus, Telegraf, InfluxQL compatibility

**Expected Outcome:** 10x storage efficiency improvement, 5x query performance gain, and enterprise-grade security posture within 12 months.

---

## Current State Analysis

### Strengths
- ✅ Distributed cluster architecture with Raft consensus
- ✅ Multiple query protocols (SQL via DataFusion, Arrow Flight, MySQL wire)
- ✅ LSM-style leveled compaction with distributed coordination
- ✅ Warm standby replication for HA
- ✅ Multiple caching layers (query results, hot partitions, metadata, Parquet files)
- ✅ Prometheus metrics endpoint
- ✅ Object store support (S3/MinIO/OSS)

### Critical Gaps
- ❌ **No downsampling** - Historical data consumes full storage
- ❌ **No TLS/SSL** - Insecure data transmission
- ❌ **No encryption at rest** - Compliance blocker
- ❌ **No RBAC** - Single user authentication only
- ❌ **Limited compression** - ZSTD only, no per-column optimization
- ❌ **No InfluxQL** - Migration barrier from InfluxDB
- ❌ **No Grafana integration** - Manual dashboard setup required
- ❌ **No backup/restore tools** - Disaster recovery gap
- ❌ **No alerting engine** - Requires external systems
- ❌ **No streaming queries** - Real-time dashboards require polling

### Competitive Positioning

| Feature | InfluxDB | TimescaleDB | VictoriaMetrics | ClickHouse | **MynaDB Current** | **MynaDB Target** |
|---------|----------|-------------|-----------------|------------|-------------------|-------------------|
| Downsampling | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ |
| TLS/Encryption | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ |
| RBAC | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ |
| InfluxQL | ✓ | ✗ | ✓ | ✗ | ✗ | ✓ |
| Grafana Plugin | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ |
| Alerting | ✓ | ✓ | ✓ | ✗ | ✗ | ✓ |
| Streaming Queries | ✓ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Backup/Restore | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ |
| Distributed Queries | ✓ | ✓ | ✓ | ✓ | Partial | ✓ |
| Compression Options | Multiple | Multiple | Custom | Multiple | ZSTD only | Multiple |

---

## Proposed Features

### Phase 1: Security & Compliance (P0) - Months 1-2

#### 1.1 TLS/SSL Encryption (CRITICAL)

**Problem:** All data transmitted in plaintext. Enterprise blocker.

**Scope:**
- HTTPS for REST API
- TLS for Arrow Flight gRPC
- SSL for MySQL wire protocol
- Certificate management (Let's Encrypt, custom CA)

**Implementation:**
```rust
// src/services/tls.rs
use rustls::{Certificate, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;

pub struct TlsConfiguration {
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_cert_path: Option<PathBuf>,
    client_auth: bool,
}

impl TlsConfiguration {
    pub async fn load_server_config(&self) -> Result<ServerConfig> {
        let certs = load_certs(&self.cert_path)?;
        let key = load_private_key(&self.key_path)?;

        let config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        Ok(config)
    }
}

// Update config.toml
[server]
tls_enabled = true
tls_cert = "/etc/mynadb/server.crt"
tls_key = "/etc/mynadb/server.key"
tls_ca_cert = "/etc/mynadb/ca.crt"  # Optional for client auth
tls_client_auth = false

[server.tls]
min_version = "1.2"  # TLS 1.2 minimum
cipher_suites = ["TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", ...]
```

**Acceptance Criteria:**
- [ ] HTTPS endpoints with valid certificates
- [ ] gRPC TLS with mutual authentication support
- [ ] MySQL SSL connections
- [ ] Automatic certificate renewal via ACME (Let's Encrypt)
- [ ] Configuration flag to enforce TLS-only connections

**Expected Impact:** Enterprise compliance requirement, security audit pass

---

#### 1.2 Encryption at Rest

**Problem:** Parquet files stored unencrypted on S3/disk. GDPR/HIPAA blocker.

**Scope:**
- Parquet file encryption (column-level)
- WAL encryption
- KMS integration (AWS KMS, HashiCorp Vault, local keys)
- Key rotation support

**Implementation:**
```rust
// src/storage/encryption.rs
use parquet::file::properties::WriterProperties;
use parquet::file::encryption::{EncryptionConfiguration, KeyManagementService};

pub struct EncryptionConfig {
    enabled: bool,
    kms_type: KmsType,
    master_key_id: String,
    encryption_algorithm: EncryptionAlgorithm,
}

pub enum KmsType {
    AwsKms { region: String },
    Vault { endpoint: String, token: String },
    Local { key_file: PathBuf },
}

pub enum EncryptionAlgorithm {
    AesGcm256,
    AesGcm128,
}

impl EncryptionConfig {
    pub fn build_parquet_encryption(&self) -> Result<EncryptionConfiguration> {
        let kms = self.init_kms()?;

        EncryptionConfiguration::builder()
            .with_column_keys(self.get_column_keys()?)
            .with_footer_key(self.get_footer_key()?)
            .with_encryption_algorithm(self.encryption_algorithm.into())
            .build()
    }

    fn init_kms(&self) -> Result<Box<dyn KeyManagementService>> {
        match &self.kms_type {
            KmsType::AwsKms { region } => {
                Ok(Box::new(AwsKmsClient::new(region)?))
            },
            KmsType::Vault { endpoint, token } => {
                Ok(Box::new(VaultKmsClient::new(endpoint, token)?))
            },
            KmsType::Local { key_file } => {
                Ok(Box::new(LocalKmsClient::from_file(key_file)?))
            }
        }
    }
}

// Update config.toml
[storage.encryption]
enabled = true
kms_type = "aws_kms"  # or "vault", "local"
master_key_id = "arn:aws:kms:us-east-1:123456789:key/..."
algorithm = "aes_gcm_256"

# Column-level encryption (optional)
encrypt_columns = ["password", "ssn", "credit_card"]
```

**WAL Encryption:**
```rust
// src/storage/wal.rs
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit};

struct EncryptedWalWriter {
    inner: BufWriter<File>,
    cipher: Aes256Gcm,
    nonce_counter: u64,
}

impl EncryptedWalWriter {
    fn write_entry(&mut self, entry: &WalEntry) -> Result<()> {
        let plaintext = bincode::serialize(entry)?;
        let nonce = self.next_nonce();
        let ciphertext = self.cipher.encrypt(&nonce, plaintext.as_ref())?;

        // Write: [nonce (12 bytes)][ciphertext length (4 bytes)][ciphertext][crc32]
        self.inner.write_all(&nonce)?;
        self.inner.write_u32::<LittleEndian>(ciphertext.len() as u32)?;
        self.inner.write_all(&ciphertext)?;
        self.write_checksum(&ciphertext)?;

        Ok(())
    }
}
```

**Acceptance Criteria:**
- [ ] Parquet files encrypted with column-level granularity
- [ ] WAL encryption with AES-256-GCM
- [ ] KMS integration (AWS KMS, Vault, local key files)
- [ ] Key rotation without downtime
- [ ] Performance overhead <10%

**Expected Impact:** GDPR/HIPAA compliance, SOC2 Type II certification path

---

#### 1.3 Multi-Tenancy & RBAC

**Problem:** Single user authentication. No isolation for SaaS deployments.

**Scope:**
- User/role management
- Database/table-level permissions (read, write, admin)
- API key authentication
- Session management
- Audit logging

**Implementation:**
```rust
// src/auth/rbac.rs
pub struct User {
    id: Uuid,
    username: String,
    password_hash: String,
    roles: Vec<Role>,
    api_keys: Vec<ApiKey>,
    created_at: DateTime<Utc>,
}

pub struct Role {
    id: Uuid,
    name: String,
    permissions: Vec<Permission>,
}

pub enum Permission {
    DatabaseRead { database: String },
    DatabaseWrite { database: String },
    DatabaseAdmin { database: String },
    TableRead { database: String, schema: String, table: String },
    TableWrite { database: String, schema: String, table: String },
    SystemAdmin,
}

pub struct AuthorizationService {
    users: DashMap<String, User>,
    roles: DashMap<String, Role>,
}

impl AuthorizationService {
    pub fn check_permission(&self, user: &User, permission: &Permission) -> Result<()> {
        for role in &user.roles {
            if role.has_permission(permission) {
                return Ok(());
            }
        }
        Err(Error::Unauthorized)
    }

    pub fn audit_log(&self, user: &User, action: &str, resource: &str) {
        let entry = AuditLogEntry {
            timestamp: Utc::now(),
            user_id: user.id,
            action: action.to_string(),
            resource: resource.to_string(),
            ip_address: self.get_client_ip(),
        };
        self.write_audit_log(entry);
    }
}

// API endpoints for user management
// POST /api/users
// POST /api/roles
// POST /api/users/:id/roles
// GET /api/audit_logs
```

**Configuration:**
```toml
[auth.rbac]
enabled = true
admin_username = "admin"
admin_password_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
session_ttl_hours = 24
audit_log_enabled = true
audit_log_path = "/var/log/mynadb/audit.log"
```

**Acceptance Criteria:**
- [ ] User CRUD operations via API
- [ ] Role-based permissions (database, table, system levels)
- [ ] API key authentication with scoped permissions
- [ ] Session management with configurable TTL
- [ ] Audit logging of all data access operations
- [ ] Integration with LDAP/Active Directory (optional)

**Expected Impact:** SaaS readiness, multi-tenant deployments, security compliance

---

### Phase 2: Storage Efficiency (P0) - Months 3-4

#### 2.1 Automated Downsampling (CRITICAL)

**Problem:** Historical data stored at full resolution. 10-100x unnecessary storage costs.

**Scope:**
- Policy-based downsampling (e.g., 1s → 1m after 7 days, 1m → 1h after 30 days)
- Per-table downsampling configurations
- Aggregate preservation (min, max, avg, sum, count, first, last)
- Automatic query rewriting to use downsampled data
- Hierarchical storage tiers

**Implementation:**
```rust
// src/downsampling/policy.rs
pub struct DownsamplingPolicy {
    source_table: TableRef,
    tiers: Vec<DownsamplingTier>,
    enabled: bool,
}

pub struct DownsamplingTier {
    name: String,
    after_days: u32,
    interval: Duration,
    aggregates: Vec<AggregateFunction>,
    retention_days: Option<u32>,
}

pub enum AggregateFunction {
    Min { column: String },
    Max { column: String },
    Avg { column: String },
    Sum { column: String },
    Count,
    First { column: String },
    Last { column: String },
}

// Example policy
let policy = DownsamplingPolicy {
    source_table: "mydb.default.cpu".into(),
    tiers: vec![
        DownsamplingTier {
            name: "tier_1m".into(),
            after_days: 7,
            interval: Duration::minutes(1),
            aggregates: vec![
                AggregateFunction::Min { column: "usage".into() },
                AggregateFunction::Max { column: "usage".into() },
                AggregateFunction::Avg { column: "usage".into() },
                AggregateFunction::First { column: "host".into() },
            ],
            retention_days: Some(90),
        },
        DownsamplingTier {
            name: "tier_1h".into(),
            after_days: 30,
            interval: Duration::hours(1),
            aggregates: vec![
                AggregateFunction::Min { column: "usage".into() },
                AggregateFunction::Max { column: "usage".into() },
                AggregateFunction::Avg { column: "usage".into() },
            ],
            retention_days: Some(365),
        },
        DownsamplingTier {
            name: "tier_1d".into(),
            after_days: 90,
            interval: Duration::days(1),
            aggregates: vec![
                AggregateFunction::Avg { column: "usage".into() },
            ],
            retention_days: None,  // Keep forever
        },
    ],
    enabled: true,
};
```

**Background Job:**
```rust
// src/jobs/downsampling.rs
pub struct DownsamplingJob {
    policies: Arc<RwLock<HashMap<TableRef, DownsamplingPolicy>>>,
    query_engine: Arc<QueryEngine>,
    interval: Duration,
}

impl DownsamplingJob {
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(self.interval).await;

            for (table, policy) in self.policies.read().await.iter() {
                for tier in &policy.tiers {
                    let cutoff_date = Utc::now() - Duration::days(tier.after_days as i64);
                    let partitions = self.find_eligible_partitions(table, cutoff_date).await?;

                    for partition in partitions {
                        if self.should_downsample(&partition, tier).await? {
                            self.downsample_partition(&partition, tier).await?;
                            self.delete_original_partition(&partition).await?;
                        }
                    }
                }
            }
        }
    }

    async fn downsample_partition(&self, partition: &Partition, tier: &DownsamplingTier) -> Result<()> {
        let sql = self.build_downsampling_query(partition, tier);
        let result = self.query_engine.execute(&sql).await?;

        // Write downsampled data to new tier table
        let tier_table = format!("{}_{}", partition.table, tier.name);
        self.write_results(&tier_table, result).await?;

        Ok(())
    }

    fn build_downsampling_query(&self, partition: &Partition, tier: &DownsamplingTier) -> String {
        let agg_columns = tier.aggregates.iter()
            .map(|agg| match agg {
                AggregateFunction::Min { column } => format!("MIN({}) as {}_min", column, column),
                AggregateFunction::Max { column } => format!("MAX({}) as {}_max", column, column),
                AggregateFunction::Avg { column } => format!("AVG({}) as {}_avg", column, column),
                AggregateFunction::Sum { column } => format!("SUM({}) as {}_sum", column, column),
                AggregateFunction::Count => "COUNT(*) as count".to_string(),
                AggregateFunction::First { column } => format!("first_value({}) as {}_first", column, column),
                AggregateFunction::Last { column } => format!("last_value({}) as {}_last", column, column),
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT time_bucket('{}', time) as time, {}, tags FROM {} WHERE date = '{}' GROUP BY time_bucket('{}', time), tags",
            tier.interval, agg_columns, partition.table, partition.date, tier.interval
        )
    }
}
```

**Query Rewriting:**
```rust
// src/query/analyzers/downsampling_rewriter.rs
pub struct DownsamplingRewriter {
    policies: Arc<RwLock<HashMap<TableRef, DownsamplingPolicy>>>,
}

impl DownsamplingRewriter {
    pub fn rewrite(&self, plan: LogicalPlan, time_range: TimeRange) -> Result<LogicalPlan> {
        // Extract table reference from plan
        let table = self.extract_table(&plan)?;

        // Check if query should use downsampled data
        if let Some(policy) = self.policies.read().unwrap().get(&table) {
            for tier in &policy.tiers {
                let cutoff = Utc::now() - Duration::days(tier.after_days as i64);

                if time_range.end < cutoff {
                    // Rewrite query to use downsampled tier table
                    return self.rewrite_to_tier(&plan, tier);
                }
            }
        }

        Ok(plan)
    }
}
```

**API Endpoints:**
```bash
# Create downsampling policy
POST /api/downsampling/policies
{
  "table": "mydb.default.cpu",
  "tiers": [
    {
      "name": "tier_1m",
      "after_days": 7,
      "interval": "1m",
      "aggregates": ["min(usage)", "max(usage)", "avg(usage)"],
      "retention_days": 90
    }
  ]
}

# List policies
GET /api/downsampling/policies

# Get policy status
GET /api/downsampling/policies/mydb.default.cpu

# Delete policy
DELETE /api/downsampling/policies/mydb.default.cpu
```

**Configuration:**
```toml
[downsampling]
enabled = true
check_interval_secs = 3600  # Check hourly
max_concurrent_jobs = 4
default_tiers = [
    { after_days = 7, interval = "1m", retention_days = 90 },
    { after_days = 30, interval = "1h", retention_days = 365 },
    { after_days = 90, interval = "1d" },
]
```

**Acceptance Criteria:**
- [ ] Policy-based downsampling with configurable tiers
- [ ] Background job for automatic downsampling
- [ ] Query rewriter to transparently use downsampled data
- [ ] API endpoints for policy management
- [ ] Storage space reduction metrics (target: 10-100x)
- [ ] Query performance maintained or improved

**Expected Impact:**
- **Storage:** 10-100x reduction for historical data
- **Cost:** Proportional S3/disk savings
- **Query Speed:** Improved for time-range queries on old data

---

#### 2.2 Adaptive Compression

**Problem:** ZSTD-only compression. Cannot optimize per-column characteristics.

**Scope:**
- Per-column compression (Snappy for hot data, ZSTD for cold, LZ4 for integers)
- Automatic compression selection based on data characteristics
- Compression policy per table/tier
- Recompression jobs for optimizing old data

**Implementation:**
```rust
// src/storage/compression.rs
pub enum CompressionStrategy {
    Fixed(Compression),
    Adaptive(AdaptiveCompressionConfig),
}

pub struct AdaptiveCompressionConfig {
    default: Compression,
    column_rules: HashMap<String, CompressionRule>,
    type_rules: HashMap<DataType, Compression>,
}

pub struct CompressionRule {
    compression: Compression,
    condition: CompressionCondition,
}

pub enum CompressionCondition {
    Always,
    Cardinality { threshold: usize },  // High cardinality → ZSTD
    Size { min_bytes: usize },  // Small columns → Snappy
    DataAge { days: u32 },  // Old data → ZSTD, new data → Snappy
}

impl CompressionStrategy {
    pub fn select_compression(&self, column: &ColumnInfo, partition_age: Duration) -> Compression {
        match self {
            Self::Fixed(c) => *c,
            Self::Adaptive(config) => {
                // Check column-specific rules
                if let Some(rule) = config.column_rules.get(&column.name) {
                    if rule.condition.matches(column, partition_age) {
                        return rule.compression;
                    }
                }

                // Check type-based rules
                if let Some(compression) = config.type_rules.get(&column.data_type) {
                    return *compression;
                }

                // Heuristics
                if partition_age.num_days() > 7 {
                    Compression::ZSTD(ZstdLevel::try_new(9).unwrap())  // Max compression for cold data
                } else if column.cardinality > 10000 {
                    Compression::ZSTD(ZstdLevel::default())  // Good for high cardinality
                } else if matches!(column.data_type, DataType::Int64 | DataType::Float64) {
                    Compression::LZ4  // Fast for numeric types
                } else {
                    Compression::SNAPPY  // Fast for recent data
                }
            }
        }
    }
}

// Update parquet writer
impl ParquetWriter {
    fn build_writer_properties(&self, schema: &Schema, partition_age: Duration) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(500_000)
            .set_data_page_size_limit(1024 * 1024);

        for (idx, field) in schema.fields().iter().enumerate() {
            let column_info = self.analyze_column(field, idx);
            let compression = self.compression_strategy.select_compression(&column_info, partition_age);
            builder = builder.set_column_compression(field.name().into(), compression);
        }

        builder.build()
    }
}
```

**Configuration:**
```toml
[storage.compression]
strategy = "adaptive"  # or "fixed"

# Adaptive compression rules
[storage.compression.adaptive]
default = "zstd"

# Per-column rules
[[storage.compression.adaptive.columns]]
pattern = "host|server|node"  # Low cardinality tags
compression = "snappy"

[[storage.compression.adaptive.columns]]
pattern = ".*_id|.*_key"  # High cardinality IDs
compression = "zstd"
zstd_level = 9

# Per-type rules
[storage.compression.adaptive.types]
int64 = "lz4"
float64 = "lz4"
string = "zstd"
boolean = "uncompressed"

# Age-based rules
[storage.compression.adaptive.age]
hot_days = 7  # < 7 days: Snappy
warm_days = 30  # 7-30 days: ZSTD level 3
cold_compression = "zstd"  # > 30 days: ZSTD level 9
```

**Recompression Job:**
```rust
// src/jobs/recompression.rs
pub struct RecompressionJob {
    interval: Duration,
    warm_cutoff_days: u32,
    cold_cutoff_days: u32,
}

impl RecompressionJob {
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(self.interval).await;

            // Find partitions that aged into new compression tier
            let partitions = self.find_partitions_needing_recompression().await?;

            for partition in partitions {
                info!("Recompressing partition: {}", partition.path.display());
                self.recompress_partition(&partition).await?;
            }
        }
    }

    async fn recompress_partition(&self, partition: &Partition) -> Result<()> {
        // Read existing Parquet files
        let batches = self.read_partition(partition).await?;

        // Determine new compression based on age
        let age = Utc::now() - partition.date;
        let compression_strategy = self.select_compression_strategy(age);

        // Rewrite with new compression
        self.write_partition(partition, batches, compression_strategy).await?;

        // Delete old files
        self.delete_old_files(partition).await?;

        Ok(())
    }
}
```

**Acceptance Criteria:**
- [ ] Per-column compression selection
- [ ] Adaptive compression based on data characteristics
- [ ] Age-based compression (hot → warm → cold)
- [ ] Recompression background job
- [ ] Compression ratio metrics per table/column
- [ ] Performance benchmarks showing optimal settings

**Expected Impact:**
- **Storage:** 20-50% additional savings beyond ZSTD-only
- **Query Speed:** 10-30% improvement on hot data (Snappy)
- **Write Speed:** 5-15% improvement on recent writes

---

### Phase 3: Query Performance & Real-Time (P1) - Months 5-6

#### 3.1 InfluxQL Support

**Problem:** Migration barrier for InfluxDB users. InfluxQL is industry standard for TSDB queries.

**Scope:**
- Full InfluxQL parser
- Query transpiler to SQL/DataFusion
- InfluxDB-compatible HTTP endpoints (`/query`, `/write`)
- Support for InfluxDB client libraries

**Implementation:**
```rust
// src/query/influxql/parser.rs
use pest::Parser;

#[derive(Parser)]
#[grammar = "query/influxql/influxql.pest"]
pub struct InfluxQLParser;

pub fn parse_influxql(query: &str) -> Result<InfluxQLQuery> {
    let pairs = InfluxQLParser::parse(Rule::query, query)?;
    // Build AST...
}

// src/query/influxql/transpiler.rs
pub struct InfluxQLTranspiler;

impl InfluxQLTranspiler {
    pub fn to_sql(&self, query: &InfluxQLQuery) -> Result<String> {
        match query {
            InfluxQLQuery::Select(stmt) => self.transpile_select(stmt),
            InfluxQLQuery::Show(stmt) => self.transpile_show(stmt),
        }
    }

    fn transpile_select(&self, stmt: &SelectStatement) -> Result<String> {
        // Example: SELECT mean(cpu) FROM telegraf WHERE time > now() - 1h GROUP BY time(5m)
        // Becomes: SELECT time_bucket('5m', time) as time, AVG(cpu) as mean
        //          FROM telegraf WHERE time > now() - interval '1 hour'
        //          GROUP BY time_bucket('5m', time)

        let select_list = self.transpile_fields(&stmt.fields)?;
        let from = self.transpile_from(&stmt.from)?;
        let where_clause = self.transpile_where(&stmt.where_clause)?;
        let group_by = self.transpile_group_by(&stmt.group_by)?;
        let order_by = self.transpile_order_by(&stmt.order_by)?;
        let limit = self.transpile_limit(&stmt.limit)?;

        Ok(format!(
            "SELECT {} FROM {} {} {} {} {}",
            select_list, from, where_clause, group_by, order_by, limit
        ))
    }
}

// src/services/influxdb_api.rs
pub async fn influxdb_query_handler(
    Query(params): Query<InfluxDBQueryParams>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse> {
    let influxql = params.q;
    let db = params.db;

    // Parse InfluxQL
    let query = parse_influxql(&influxql)?;

    // Transpile to SQL
    let sql = InfluxQLTranspiler.to_sql(&query)?;

    // Execute via QueryEngine
    let results = state.query_engine.execute(&db, &sql).await?;

    // Format as InfluxDB JSON response
    let response = format_influxdb_response(results)?;
    Ok(Json(response))
}
```

**API Compatibility:**
```bash
# InfluxDB-compatible endpoints
GET  /query?db=mydb&q=SELECT+mean(cpu)+FROM+telegraf+WHERE+time+>+now()-1h+GROUP+BY+time(5m)
POST /query   # Body: { "db": "mydb", "q": "..." }
POST /write?db=mydb   # Line protocol
GET  /ping
```

**Configuration:**
```toml
[services.influxdb]
enabled = true
endpoint = "/influxdb"
# Map InfluxDB functions to SQL
[services.influxdb.function_mapping]
mean = "AVG"
median = "MEDIAN"
spread = "MAX({}) - MIN({})"
derivative = "({} - LAG({}, 1)) / ({time} - LAG({time}, 1))"
```

**Acceptance Criteria:**
- [ ] Parser for InfluxQL SELECT, SHOW, CREATE, DROP statements
- [ ] Transpiler to SQL with >90% InfluxQL syntax coverage
- [ ] InfluxDB-compatible HTTP endpoints
- [ ] Support for InfluxDB client libraries (Go, Python, JavaScript)
- [ ] Time range expressions (`now() - 1h`, `2024-01-01T00:00:00Z`)
- [ ] GROUP BY time() with fill policies (null, 0, previous, linear)
- [ ] Aggregate functions (mean, median, mode, stddev, etc.)

**Expected Impact:**
- **Adoption:** Major reduction in migration effort from InfluxDB
- **Compatibility:** Existing InfluxDB dashboards/scripts work without changes

---

#### 3.2 Streaming Queries & Subscriptions

**Problem:** Real-time dashboards require polling. No push-based data delivery.

**Scope:**
- Continuous queries that publish results on new data
- WebSocket subscriptions for query results
- Server-Sent Events (SSE) for HTTP streaming
- Backpressure handling

**Implementation:**
```rust
// src/query/streaming.rs
pub struct StreamingQueryManager {
    subscriptions: Arc<DashMap<Uuid, Subscription>>,
    query_engine: Arc<QueryEngine>,
    write_cache: Arc<DataCache>,
}

pub struct Subscription {
    id: Uuid,
    query: String,
    database: String,
    sender: mpsc::Sender<QueryResult>,
    created_at: Instant,
}

impl StreamingQueryManager {
    pub async fn subscribe(&self, db: &str, sql: &str) -> Result<SubscriptionStream> {
        let (tx, rx) = mpsc::channel(100);

        let subscription = Subscription {
            id: Uuid::new_v4(),
            query: sql.to_string(),
            database: db.to_string(),
            sender: tx,
            created_at: Instant::now(),
        };

        self.subscriptions.insert(subscription.id, subscription);

        // Initial query execution
        let initial_results = self.query_engine.execute(db, sql).await?;
        let _ = tx.send(QueryResult::Initial(initial_results)).await;

        Ok(SubscriptionStream { id: subscription.id, receiver: rx })
    }

    pub async fn on_data_written(&self, db: &str, table: &str) {
        // Find subscriptions that may be affected by this write
        let affected = self.subscriptions.iter()
            .filter(|s| s.database == db && s.query.contains(table))
            .collect::<Vec<_>>();

        for subscription in affected {
            // Re-execute query and send incremental results
            if let Ok(results) = self.query_engine.execute(&subscription.database, &subscription.query).await {
                let _ = subscription.sender.send(QueryResult::Update(results)).await;
            }
        }
    }
}

// WebSocket handler
pub async fn ws_query_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_query_stream(socket, state))
}

async fn handle_query_stream(mut socket: WebSocket, state: Arc<AppState>) {
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            let req: StreamQueryRequest = serde_json::from_str(&text).unwrap();

            // Subscribe to query
            let mut stream = state.streaming_manager
                .subscribe(&req.database, &req.query)
                .await
                .unwrap();

            // Stream results
            while let Some(result) = stream.next().await {
                let json = serde_json::to_string(&result).unwrap();
                if socket.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    }
}

// Server-Sent Events handler
pub async fn sse_query_handler(
    Query(params): Query<QueryParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let stream = state.streaming_manager
        .subscribe(&params.db, &params.sql)
        .await?;

    let event_stream = stream.map(|result| {
        let data = serde_json::to_string(&result).unwrap();
        Ok(Event::default().data(data))
    });

    Ok(Sse::new(event_stream))
}
```

**API Endpoints:**
```bash
# WebSocket streaming query
WS /stream/query
> {"database": "mydb", "query": "SELECT * FROM cpu WHERE host = 'server01'"}
< {"type": "initial", "data": [...]}
< {"type": "update", "data": [{"time": ..., "cpu": 85.2}]}

# Server-Sent Events
GET /stream/query/mydb?sql=SELECT+*+FROM+cpu
data: {"time": 1234567890, "cpu": 85.2}
```

**Configuration:**
```toml
[query.streaming]
enabled = true
max_subscriptions_per_connection = 10
max_total_subscriptions = 10000
update_debounce_ms = 100  # Batch updates within 100ms
backpressure_threshold = 1000  # Buffer size before dropping
```

**Acceptance Criteria:**
- [ ] WebSocket endpoint for streaming queries
- [ ] SSE endpoint for HTTP/2 streaming
- [ ] Publish query results on relevant data writes
- [ ] Backpressure handling (drop old updates if consumer slow)
- [ ] Subscription management (list, cancel)
- [ ] Connection limits and rate limiting

**Expected Impact:**
- **Real-time dashboards:** Sub-second update latency
- **Reduced load:** Eliminate polling queries
- **User experience:** Live updates without refresh

---

#### 3.3 Built-in Alerting Engine

**Problem:** Requires external alerting systems (Prometheus Alertmanager, Grafana alerts).

**Scope:**
- Rule-based alerts (threshold, rate-of-change, anomaly detection)
- Notification channels (email, webhook, Slack, PagerDuty)
- Alert history and state management
- Silencing and inhibition rules

**Implementation:**
```rust
// src/alerting/rule.rs
pub struct AlertRule {
    id: Uuid,
    name: String,
    database: String,
    query: String,
    condition: AlertCondition,
    notification_channels: Vec<NotificationChannel>,
    evaluation_interval: Duration,
    for_duration: Duration,  // Must be in alert state for this long
    labels: HashMap<String, String>,
    annotations: HashMap<String, String>,
}

pub enum AlertCondition {
    Threshold {
        column: String,
        operator: ComparisonOperator,
        value: f64,
    },
    RateOfChange {
        column: String,
        window: Duration,
        threshold: f64,
    },
    Anomaly {
        column: String,
        sensitivity: f64,  // Z-score threshold
    },
    NoData {
        window: Duration,
    },
}

pub enum NotificationChannel {
    Email { addresses: Vec<String> },
    Webhook { url: String, method: HttpMethod, headers: HashMap<String, String> },
    Slack { webhook_url: String, channel: String },
    PagerDuty { integration_key: String },
}

// src/alerting/engine.rs
pub struct AlertingEngine {
    rules: Arc<RwLock<HashMap<Uuid, AlertRule>>>,
    alert_states: Arc<DashMap<Uuid, AlertState>>,
    query_engine: Arc<QueryEngine>,
    notification_service: Arc<NotificationService>,
}

pub struct AlertState {
    rule_id: Uuid,
    state: State,
    active_since: Option<Instant>,
    last_notification: Option<Instant>,
    notification_count: u32,
}

pub enum State {
    Inactive,
    Pending,
    Firing,
    Resolved,
}

impl AlertingEngine {
    pub async fn run(&self) {
        loop {
            for (id, rule) in self.rules.read().await.iter() {
                self.evaluate_rule(rule).await;
            }
            tokio::time::sleep(Duration::seconds(10)).await;
        }
    }

    async fn evaluate_rule(&self, rule: &AlertRule) {
        // Execute alert query
        let results = match self.query_engine.execute(&rule.database, &rule.query).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Alert query failed: {}: {}", rule.name, e);
                return;
            }
        };

        // Check condition
        let triggered = self.check_condition(&results, &rule.condition);

        // Update alert state
        let mut state = self.alert_states.entry(rule.id)
            .or_insert(AlertState::inactive());

        match (state.state, triggered) {
            (State::Inactive, true) => {
                state.state = State::Pending;
                state.active_since = Some(Instant::now());
            }
            (State::Pending, true) => {
                if state.active_since.unwrap().elapsed() >= rule.for_duration {
                    state.state = State::Firing;
                    self.send_notifications(rule, &results).await;
                }
            }
            (State::Firing, false) => {
                state.state = State::Resolved;
                self.send_resolution_notifications(rule).await;
                state.active_since = None;
            }
            (State::Resolved, false) => {
                state.state = State::Inactive;
            }
            _ => {}
        }
    }

    fn check_condition(&self, results: &[RecordBatch], condition: &AlertCondition) -> bool {
        match condition {
            AlertCondition::Threshold { column, operator, value } => {
                // Extract column values and check against threshold
                for batch in results {
                    if let Some(col) = batch.column_by_name(column) {
                        let values = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        for v in values.iter().flatten() {
                            if operator.evaluate(v, *value) {
                                return true;
                            }
                        }
                    }
                }
                false
            }
            AlertCondition::RateOfChange { column, window, threshold } => {
                // Calculate rate of change over window
                // ...
            }
            AlertCondition::Anomaly { column, sensitivity } => {
                // Z-score anomaly detection
                // ...
            }
            AlertCondition::NoData { window } => {
                results.is_empty()
            }
        }
    }

    async fn send_notifications(&self, rule: &AlertRule, results: &[RecordBatch]) {
        let alert = Alert {
            rule_name: rule.name.clone(),
            labels: rule.labels.clone(),
            annotations: rule.annotations.clone(),
            value: self.extract_alert_value(results),
            fired_at: Utc::now(),
        };

        for channel in &rule.notification_channels {
            self.notification_service.send(channel, &alert).await;
        }
    }
}

// src/alerting/notification.rs
pub struct NotificationService;

impl NotificationService {
    pub async fn send(&self, channel: &NotificationChannel, alert: &Alert) -> Result<()> {
        match channel {
            NotificationChannel::Email { addresses } => {
                self.send_email(addresses, alert).await
            }
            NotificationChannel::Webhook { url, method, headers } => {
                self.send_webhook(url, method, headers, alert).await
            }
            NotificationChannel::Slack { webhook_url, channel } => {
                self.send_slack(webhook_url, channel, alert).await
            }
            NotificationChannel::PagerDuty { integration_key } => {
                self.send_pagerduty(integration_key, alert).await
            }
        }
    }

    async fn send_slack(&self, webhook_url: &str, channel: &str, alert: &Alert) -> Result<()> {
        let payload = json!({
            "channel": channel,
            "text": format!("🔥 Alert: {}", alert.rule_name),
            "attachments": [{
                "color": "danger",
                "fields": [
                    {"title": "Value", "value": alert.value, "short": true},
                    {"title": "Time", "value": alert.fired_at.to_rfc3339(), "short": true}
                ]
            }]
        });

        self.http_client.post(webhook_url)
            .json(&payload)
            .send()
            .await?;

        Ok(())
    }
}
```

**API Endpoints:**
```bash
# Create alert rule
POST /api/alerts/rules
{
  "name": "High CPU Usage",
  "database": "mydb",
  "query": "SELECT AVG(cpu) as avg_cpu FROM cpu WHERE time > now() - 5m",
  "condition": {
    "type": "threshold",
    "column": "avg_cpu",
    "operator": ">",
    "value": 90.0
  },
  "evaluation_interval": "1m",
  "for": "5m",
  "notification_channels": [
    {
      "type": "slack",
      "webhook_url": "https://hooks.slack.com/...",
      "channel": "#alerts"
    }
  ],
  "labels": {"severity": "high", "team": "platform"}
}

# List rules
GET /api/alerts/rules

# Get rule status
GET /api/alerts/rules/:id

# List active alerts
GET /api/alerts/active

# Silence alert
POST /api/alerts/silence
{
  "rule_id": "...",
  "duration": "2h",
  "reason": "Maintenance window"
}
```

**Configuration:**
```toml
[alerting]
enabled = true
evaluation_interval_secs = 10
max_concurrent_evaluations = 100

[alerting.smtp]
host = "smtp.gmail.com"
port = 587
username = "alerts@example.com"
password = "..."
from = "MynaDB Alerts <alerts@example.com>"
```

**Acceptance Criteria:**
- [ ] Rule-based alerts with multiple condition types
- [ ] Email, webhook, Slack, PagerDuty notification channels
- [ ] Alert state management (pending → firing → resolved)
- [ ] Alert history and audit log
- [ ] Silencing and inhibition rules
- [ ] Alert grouping and deduplication
- [ ] API for rule management

**Expected Impact:**
- **Self-contained:** No external alerting system required
- **Ops efficiency:** Instant notifications on anomalies
- **Reliability:** Catch incidents before customer impact

---

### Phase 4: Ecosystem Integration (P1) - Months 7-9

#### 4.1 Grafana Datasource Plugin

**Problem:** Manual Grafana datasource setup. No native MynaDB support.

**Scope:**
- Official Grafana plugin (TypeScript)
- Query builder UI for MynaDB-specific features
- Variable support ($host, $interval, etc.)
- Time range macros
- Alerting integration

**Implementation:**
```typescript
// src/datasource.ts
import { DataSourceInstanceSettings, DataQueryRequest, DataQueryResponse } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

export class MynaDBDatasource extends DataSourceWithBackend<MynaDBQuery, MynaDBOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<MynaDBOptions>) {
    super(instanceSettings);
  }

  async query(request: DataQueryRequest<MynaDBQuery>): Promise<DataQueryResponse> {
    // Replace Grafana variables
    const queries = request.targets.map(target => ({
      ...target,
      rawSql: this.replaceVariables(target.rawSql, request.scopedVars, request.range),
    }));

    return super.query({ ...request, targets: queries });
  }

  async metricFindQuery(query: string): Promise<MetricFindValue[]> {
    // Support for template variables
    // Example: SELECT DISTINCT host FROM cpu
    const response = await this.postResource('query', { query });
    return response.data.map(row => ({ text: row[0], value: row[0] }));
  }

  replaceVariables(sql: string, scopedVars: any, timeRange: TimeRange): string {
    // Replace $__timeFilter
    sql = sql.replace(/\$__timeFilter\((\w+)\)/g,
      `$1 >= ${timeRange.from.valueOf() * 1000000} AND $1 <= ${timeRange.to.valueOf() * 1000000}`);

    // Replace $__interval
    const interval = this.calculateInterval(timeRange);
    sql = sql.replace(/\$__interval/g, interval);

    // Replace custom variables
    Object.keys(scopedVars).forEach(key => {
      const value = scopedVars[key].value;
      sql = sql.replace(new RegExp(`\\$${key}`, 'g'), value);
    });

    return sql;
  }
}

// src/QueryEditor.tsx
export function QueryEditor({ query, onChange, onRunQuery }: QueryEditorProps) {
  return (
    <div>
      <InlineField label="Query Mode">
        <Select
          value={query.queryMode}
          options={[
            { label: 'SQL', value: 'sql' },
            { label: 'InfluxQL', value: 'influxql' },
            { label: 'Builder', value: 'builder' },
          ]}
          onChange={v => onChange({ ...query, queryMode: v.value })}
        />
      </InlineField>

      {query.queryMode === 'sql' && (
        <CodeEditor
          value={query.rawSql}
          language="sql"
          onChange={v => onChange({ ...query, rawSql: v })}
          onBlur={onRunQuery}
        />
      )}

      {query.queryMode === 'builder' && (
        <QueryBuilder query={query} onChange={onChange} />
      )}
    </div>
  );
}
```

**Plugin Manifest:**
```json
{
  "name": "MynaDB",
  "id": "mynadb-datasource",
  "type": "datasource",
  "metrics": true,
  "backend": true,
  "executable": "gpx_mynadb",
  "info": {
    "description": "MynaDB time series database datasource",
    "author": { "name": "MynaDB Team" },
    "keywords": ["mynadb", "timeseries", "sql"],
    "version": "1.0.0"
  },
  "dependencies": {
    "grafanaDependency": ">=9.0.0"
  }
}
```

**Acceptance Criteria:**
- [ ] TypeScript Grafana plugin with query editor
- [ ] SQL and InfluxQL query modes
- [ ] Visual query builder for time series queries
- [ ] Grafana variable support ($__timeFilter, $__interval, custom vars)
- [ ] Alerting integration
- [ ] Published to Grafana plugin marketplace
- [ ] Documentation with dashboard examples

**Expected Impact:**
- **Adoption:** Major onboarding simplification
- **User Experience:** Native Grafana integration
- **Visibility:** Grafana marketplace listing

---

#### 4.2 Prometheus Remote Storage

**Problem:** Cannot use MynaDB as long-term storage for Prometheus.

**Scope:**
- Prometheus remote write API
- Prometheus remote read API
- Label/tag mapping
- Downsampling integration for Prometheus data

**Implementation:**
```rust
// src/services/prometheus.rs
use prometheus_parse::{Sample, Scrape};

pub async fn remote_write_handler(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<impl IntoResponse> {
    // Decode Prometheus protobuf (Snappy compressed)
    let decoded = snap::raw::Decoder::new().decompress_vec(&body)?;
    let write_request = prometheus::WriteRequest::decode(&decoded[..])?;

    // Convert to line protocol
    let mut lines = Vec::new();
    for timeseries in write_request.timeseries {
        let measurement = extract_measurement(&timeseries.labels)?;
        let tags = extract_tags(&timeseries.labels)?;

        for sample in timeseries.samples {
            let line = format!(
                "{},{} value={} {}",
                measurement,
                tags.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(","),
                sample.value,
                sample.timestamp * 1_000_000  // ms → ns
            );
            lines.push(line);
        }
    }

    // Write to MynaDB
    let database = "prometheus";  // Configurable
    state.storage.write_line_protocol(database, &lines.join("\n")).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn remote_read_handler(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<impl IntoResponse> {
    // Decode Prometheus protobuf (Snappy compressed)
    let decoded = snap::raw::Decoder::new().decompress_vec(&body)?;
    let read_request = prometheus::ReadRequest::decode(&decoded[..])?;

    // Convert to SQL queries
    let mut all_results = Vec::new();
    for query in read_request.queries {
        let sql = prometheus_to_sql(&query)?;
        let results = state.query_engine.execute("prometheus", &sql).await?;
        all_results.push(results_to_timeseries(results, &query.matchers));
    }

    // Encode response
    let read_response = prometheus::ReadResponse {
        results: all_results,
    };
    let encoded = read_response.encode_to_vec();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded)?;

    Ok((
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        compressed,
    ))
}

fn prometheus_to_sql(query: &prometheus::Query) -> Result<String> {
    // Example: {__name__="http_requests_total", job="api-server"}
    // Becomes: SELECT time, value FROM http_requests_total WHERE job = 'api-server'

    let mut conditions = Vec::new();
    let mut measurement = None;

    for matcher in &query.matchers {
        if matcher.name == "__name__" {
            measurement = Some(matcher.value.clone());
        } else {
            let op = match matcher.type_ {
                0 => "=",  // EQ
                1 => "!=", // NEQ
                2 => "~",  // REGEX (need to translate)
                3 => "!~", // NOT_REGEX
                _ => "=",
            };
            conditions.push(format!("{} {} '{}'", matcher.name, op, matcher.value));
        }
    }

    let table = measurement.ok_or(Error::MissingMetricName)?;
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {} AND time >= {} AND time <= {}",
            conditions.join(" AND "),
            query.start_timestamp_ms * 1_000_000,
            query.end_timestamp_ms * 1_000_000)
    };

    Ok(format!("SELECT time, value FROM {} {}", table, where_clause))
}
```

**Prometheus Configuration:**
```yaml
# prometheus.yml
remote_write:
  - url: http://mynadb:7000/api/v1/write
    queue_config:
      capacity: 10000
      max_samples_per_send: 5000
      batch_send_deadline: 5s

remote_read:
  - url: http://mynadb:7000/api/v1/read
    read_recent: true
```

**Acceptance Criteria:**
- [ ] Prometheus remote write endpoint
- [ ] Prometheus remote read endpoint
- [ ] Label to tag mapping
- [ ] Support for PromQL label matchers (=, !=, =~, !~)
- [ ] High-throughput write handling (>100k samples/sec)
- [ ] Downsampling for long-term Prometheus data
- [ ] Documentation for Prometheus integration

**Expected Impact:**
- **Use Case:** Long-term Prometheus storage
- **Compatibility:** Drop-in replacement for Prometheus TSDB
- **Cost Savings:** Cheaper storage vs. Thanos/Cortex

---

### Phase 5: Operational Excellence (P2) - Months 10-12

#### 5.1 Backup & Restore

**Problem:** No disaster recovery tools. Manual S3 snapshots required.

**Scope:**
- Full database backups
- Incremental backups (WAL archiving)
- Point-in-time recovery (PITR)
- Backup to S3/local filesystem
- CLI tools for backup management

**Implementation:**
```rust
// src/backup/manager.rs
pub struct BackupManager {
    data_dir: PathBuf,
    backup_dir: PathBuf,
    wal_archive_dir: PathBuf,
    object_store: Arc<dyn ObjectStore>,
}

pub struct BackupOptions {
    backup_type: BackupType,
    database: Option<String>,
    compression: bool,
    encryption: Option<EncryptionKey>,
}

pub enum BackupType {
    Full,
    Incremental { base_backup_id: Uuid },
}

impl BackupManager {
    pub async fn create_backup(&self, options: BackupOptions) -> Result<BackupMetadata> {
        let backup_id = Uuid::new_v4();
        let timestamp = Utc::now();

        match options.backup_type {
            BackupType::Full => self.create_full_backup(backup_id, &options).await,
            BackupType::Incremental { base_backup_id } => {
                self.create_incremental_backup(backup_id, base_backup_id, &options).await
            }
        }
    }

    async fn create_full_backup(&self, backup_id: Uuid, options: &BackupOptions) -> Result<BackupMetadata> {
        info!("Creating full backup: {}", backup_id);

        // 1. Flush write cache to ensure consistency
        self.flush_all_caches().await?;

        // 2. Create snapshot of data directory
        let snapshot_path = self.create_snapshot(&backup_id).await?;

        // 3. Compress if requested
        let archive_path = if options.compression {
            self.compress_snapshot(&snapshot_path).await?
        } else {
            snapshot_path
        };

        // 4. Encrypt if requested
        let final_path = if let Some(key) = &options.encryption {
            self.encrypt_backup(&archive_path, key).await?
        } else {
            archive_path
        };

        // 5. Upload to object store
        self.upload_backup(&final_path, &backup_id).await?;

        // 6. Record metadata
        let metadata = BackupMetadata {
            backup_id,
            backup_type: BackupType::Full,
            timestamp: Utc::now(),
            size_bytes: tokio::fs::metadata(&final_path).await?.len(),
            database: options.database.clone(),
            compression: options.compression,
            encrypted: options.encryption.is_some(),
        };

        self.save_metadata(&metadata).await?;

        Ok(metadata)
    }

    async fn create_incremental_backup(
        &self,
        backup_id: Uuid,
        base_backup_id: Uuid,
        options: &BackupOptions,
    ) -> Result<BackupMetadata> {
        info!("Creating incremental backup: {} (base: {})", backup_id, base_backup_id);

        // Get base backup metadata
        let base_metadata = self.get_metadata(&base_backup_id).await?;

        // Archive WAL files since base backup
        let wal_files = self.find_wal_files_since(base_metadata.timestamp).await?;

        // Copy WAL files to backup location
        for wal_file in wal_files {
            self.copy_to_backup(&wal_file, &backup_id).await?;
        }

        // Upload to object store
        self.upload_backup(&self.get_backup_path(&backup_id), &backup_id).await?;

        // Record metadata
        let metadata = BackupMetadata {
            backup_id,
            backup_type: BackupType::Incremental { base_backup_id },
            timestamp: Utc::now(),
            size_bytes: self.calculate_backup_size(&backup_id).await?,
            database: options.database.clone(),
            compression: options.compression,
            encrypted: options.encryption.is_some(),
        };

        self.save_metadata(&metadata).await?;

        Ok(metadata)
    }

    pub async fn restore_backup(&self, backup_id: Uuid, target_time: Option<DateTime<Utc>>) -> Result<()> {
        info!("Restoring backup: {}", backup_id);

        // 1. Download backup from object store
        let backup_path = self.download_backup(&backup_id).await?;

        // 2. Decrypt if needed
        let decrypted_path = self.decrypt_if_needed(&backup_path, &backup_id).await?;

        // 3. Decompress if needed
        let extracted_path = self.decompress_if_needed(&decrypted_path).await?;

        // 4. Stop database (if running)
        self.stop_database().await?;

        // 5. Clear data directory
        tokio::fs::remove_dir_all(&self.data_dir).await?;
        tokio::fs::create_dir_all(&self.data_dir).await?;

        // 6. Copy backup to data directory
        self.copy_recursive(&extracted_path, &self.data_dir).await?;

        // 7. If PITR requested, replay WAL files
        if let Some(target_time) = target_time {
            self.replay_wal_to_time(backup_id, target_time).await?;
        }

        // 8. Start database
        self.start_database().await?;

        Ok(())
    }

    async fn replay_wal_to_time(&self, backup_id: Uuid, target_time: DateTime<Utc>) -> Result<()> {
        // Find all incremental backups after base backup
        let incremental_backups = self.find_incremental_backups(&backup_id).await?;

        for incremental_backup_id in incremental_backups {
            let wal_files = self.get_wal_files(&incremental_backup_id).await?;

            for wal_file in wal_files {
                let entries = self.read_wal_file(&wal_file).await?;

                for entry in entries {
                    if entry.timestamp > target_time {
                        info!("Reached target time, stopping WAL replay");
                        return Ok(());
                    }

                    // Replay entry
                    self.replay_wal_entry(&entry).await?;
                }
            }
        }

        Ok(())
    }
}

// CLI commands
// mynadb backup create --database mydb --output s3://backups/
// mynadb backup create --incremental --base-id <uuid>
// mynadb backup list
// mynadb backup restore --backup-id <uuid>
// mynadb backup restore --backup-id <uuid> --point-in-time 2024-01-15T12:00:00Z
```

**Configuration:**
```toml
[backup]
enabled = true
backup_dir = "/var/backups/mynadb"
wal_archive_dir = "/var/backups/mynadb/wal"
schedule = "0 2 * * *"  # Daily at 2 AM
retention_days = 30
compression = true
encryption = true
encryption_key_file = "/etc/mynadb/backup.key"

[backup.s3]
bucket = "mynadb-backups"
prefix = "prod/"
```

**Acceptance Criteria:**
- [ ] Full backup creation (snapshot data directory)
- [ ] Incremental backup (WAL archiving)
- [ ] Point-in-time recovery to specific timestamp
- [ ] Compression (gzip, zstd) and encryption support
- [ ] Upload to S3/local filesystem
- [ ] Scheduled backups via cron-like syntax
- [ ] CLI tools for backup management
- [ ] Backup verification and integrity checks

**Expected Impact:**
- **Disaster Recovery:** RPO <1 hour, RTO <30 minutes
- **Compliance:** Backup retention policies
- **Peace of Mind:** Automated DR testing

---

#### 5.2 Query Tracing & Profiling

**Problem:** Difficult to debug slow queries in production.

**Scope:**
- OpenTelemetry integration for distributed tracing
- Query execution timeline visualization
- Slow query logging
- Query plan capture for slow queries
- Integration with Jaeger/Zipkin

**Implementation:**
```rust
// src/query/tracing.rs
use opentelemetry::{global, trace::{Tracer, Span, SpanKind}};
use opentelemetry_otlp::WithExportConfig;
use tracing_opentelemetry::OpenTelemetryLayer;

pub struct QueryTracer {
    tracer: Box<dyn Tracer>,
    slow_query_threshold: Duration,
}

impl QueryTracer {
    pub fn init(config: &TracingConfig) -> Result<Self> {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(&config.otlp_endpoint),
            )
            .install_batch(opentelemetry::runtime::Tokio)?;

        Ok(Self {
            tracer: Box::new(tracer),
            slow_query_threshold: config.slow_query_threshold,
        })
    }

    pub fn trace_query<F, T>(&self, database: &str, sql: &str, f: F) -> Result<(T, QueryProfile)>
    where
        F: FnOnce() -> Result<T>,
    {
        let mut span = self.tracer.start_with_kind("query.execute", SpanKind::Server);
        span.set_attribute("db.system", "mynadb");
        span.set_attribute("db.name", database.to_string());
        span.set_attribute("db.statement", sql.to_string());

        let start = Instant::now();
        let result = f();
        let duration = start.elapsed();

        span.set_attribute("query.duration_ms", duration.as_millis() as i64);

        if duration > self.slow_query_threshold {
            self.log_slow_query(database, sql, duration);
        }

        let profile = QueryProfile {
            duration,
            stages: self.extract_stages(&span),
        };

        span.end();

        Ok((result?, profile))
    }
}

// Integrate into QueryEngine
impl QueryEngine {
    pub async fn execute_traced(&self, database: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        let span = info_span!("query.execute", db = database, sql = sql);

        async move {
            // Parse & plan
            let plan = {
                let _parse_span = info_span!("query.parse").entered();
                self.parse_sql(sql).await?
            };

            // Optimize
            let optimized = {
                let _optimize_span = info_span!("query.optimize").entered();
                self.optimize_plan(plan).await?
            };

            // Execute
            let results = {
                let _execute_span = info_span!("query.execute_plan").entered();
                self.execute_plan(optimized).await?
            };

            Ok(results)
        }
        .instrument(span)
        .await
    }
}
```

**Slow Query Log:**
```rust
// src/query/slow_log.rs
pub struct SlowQueryLogger {
    log_file: PathBuf,
    threshold: Duration,
}

pub struct SlowQueryEntry {
    timestamp: DateTime<Utc>,
    database: String,
    query: String,
    duration: Duration,
    plan: String,
    row_count: usize,
    bytes_scanned: usize,
}

impl SlowQueryLogger {
    pub fn log(&self, entry: SlowQueryEntry) {
        let json = serde_json::to_string(&entry).unwrap();
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_file)
            .unwrap();
        writeln!(file, "{}", json).unwrap();
    }
}
```

**Configuration:**
```toml
[query.tracing]
enabled = true
otlp_endpoint = "http://jaeger:4317"
slow_query_threshold_ms = 1000
slow_query_log = "/var/log/mynadb/slow_query.log"
capture_plan_for_slow_queries = true
```

**Acceptance Criteria:**
- [ ] OpenTelemetry integration with Jaeger/Zipkin
- [ ] Query execution spans (parse, optimize, execute stages)
- [ ] Slow query logging with threshold
- [ ] Automatic query plan capture for slow queries
- [ ] Distributed tracing across cluster nodes
- [ ] Query profile API endpoint

**Expected Impact:**
- **Debugging:** Identify bottlenecks in production queries
- **Optimization:** Data-driven query tuning
- **SLA Compliance:** Track P50/P95/P99 query latency

---

## Implementation Roadmap

### Month 1-2: Security Foundation (P0)
- **Week 1-2:** TLS/SSL for all protocols
- **Week 3-4:** Encryption at rest (Parquet + WAL)
- **Week 5-8:** RBAC & multi-tenancy

**Deliverables:**
- ✅ HTTPS, gRPC TLS, MySQL SSL
- ✅ KMS integration (AWS KMS, Vault, local)
- ✅ User/role management API
- ✅ Audit logging

---

### Month 3-4: Storage Efficiency (P0)
- **Week 9-12:** Downsampling engine & policies
- **Week 13-16:** Adaptive compression

**Deliverables:**
- ✅ Automated downsampling with multi-tier policies
- ✅ Query rewriter for transparent downsampled data use
- ✅ Per-column compression strategies
- ✅ 10-100x storage reduction on historical data

---

### Month 5-6: Query Performance & Real-Time (P1)
- **Week 17-20:** InfluxQL parser & transpiler
- **Week 21-22:** Streaming queries (WebSocket, SSE)
- **Week 23-24:** Built-in alerting engine

**Deliverables:**
- ✅ InfluxQL support with 90%+ syntax coverage
- ✅ WebSocket streaming queries
- ✅ Alert rules with email/Slack/webhook notifications

---

### Month 7-9: Ecosystem Integration (P1)
- **Week 25-28:** Grafana datasource plugin
- **Week 29-32:** Prometheus remote storage
- **Week 33-36:** Telegraf output plugin

**Deliverables:**
- ✅ Grafana plugin published to marketplace
- ✅ Prometheus remote write/read endpoints
- ✅ Telegraf native output

---

### Month 10-12: Operational Excellence (P2)
- **Week 37-40:** Backup & restore tools
- **Week 41-44:** Query tracing & profiling
- **Week 45-48:** Performance benchmarking & tuning

**Deliverables:**
- ✅ Full/incremental backup with PITR
- ✅ OpenTelemetry integration
- ✅ Comprehensive benchmarks vs. InfluxDB/TimescaleDB

---

## Success Metrics

### Storage Efficiency
- [ ] **10-100x reduction** in storage costs for >30 day old data
- [ ] **20-50% additional savings** from adaptive compression
- [ ] **<5% query performance degradation** on downsampled data

### Query Performance
- [ ] **Match or exceed** InfluxDB query latency benchmarks
- [ ] **<100ms P95 latency** for streaming query updates
- [ ] **>10k concurrent subscriptions** supported

### Security & Compliance
- [ ] **Pass enterprise security audit** (SOC2, ISO 27001)
- [ ] **Zero critical vulnerabilities** in production deployments
- [ ] **100% of data encrypted** in transit and at rest

### Ecosystem Adoption
- [ ] **>1000 Grafana plugin installs** in first 6 months
- [ ] **>500 Prometheus integrations** deployed
- [ ] **>100 InfluxDB migrations** completed

### Operational Excellence
- [ ] **RPO <1 hour, RTO <30 minutes** for disaster recovery
- [ ] **P99 query latency tracked** and alerted on
- [ ] **Zero data loss** in production incidents

---

## Risks & Mitigations

### Risk: Downsampling Breaks Existing Queries
**Mitigation:**
- Transparent query rewriting (users unaware of downsampling)
- Feature flag for gradual rollout
- Extensive testing with real workloads

### Risk: InfluxQL Transpiler Incomplete
**Mitigation:**
- Start with 80/20 rule (most common queries)
- Clear documentation of unsupported syntax
- Fallback to SQL for advanced users

### Risk: Performance Regression from Tracing
**Mitigation:**
- Sampling (trace 1% of queries)
- Async telemetry export
- Benchmarking before/after

### Risk: RBAC Complexity Slows Queries
**Mitigation:**
- Permission caching
- Database-level coarse-grained permissions (not row-level)
- Performance benchmarks with RBAC enabled

---

## Competitive Differentiation

After implementing this proposal, MynaDB will differentiate through:

1. **Unified Architecture:** Cluster + downsampling + alerting in one binary (vs. InfluxDB Enterprise's multiple services)
2. **SQL + InfluxQL:** Flexibility for users (vs. VictoriaMetrics PromQL-only)
3. **Built-in Alerting:** No external systems needed (vs. TimescaleDB requiring Grafana)
4. **Rust Performance:** Memory safety + speed (vs. Go-based competitors)
5. **Cloud-Native:** S3/MinIO native storage (vs. ClickHouse disk-centric)
6. **Open Source:** Full feature parity in OSS (vs. InfluxDB Cloud-only features)

---

## Conclusion

This proposal transforms MynaDB from a capable TSDB into a **state-of-the-art, enterprise-ready time series platform**. By focusing on storage efficiency, query performance, security, and ecosystem integration, MynaDB will compete directly with InfluxDB, TimescaleDB, and VictoriaMetrics.

**Next Steps:**
1. Approve proposal and prioritize features
2. Allocate engineering resources (2-3 engineers for 12 months)
3. Begin Phase 1 (Security Foundation) immediately
4. Establish benchmarking infrastructure for continuous validation
5. Engage early adopters for beta testing

**Total Estimated Effort:** 36 engineer-months (3 engineers × 12 months)

**Expected ROI:**
- **10x storage savings** → Reduced infrastructure costs
- **Enterprise readiness** → Unlock Fortune 500 customers
- **Ecosystem integration** → 10x faster adoption rate
