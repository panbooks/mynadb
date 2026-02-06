//! Prometheus Remote Storage API
//!
//! Implements Prometheus remote read/write protocol, allowing MynaDB
//! to act as a drop-in replacement for Prometheus TSDB.
//!
//! Endpoints:
//! - POST /api/v1/write - Remote write (snappy-compressed protobuf)
//! - POST /api/v1/read - Remote read (protobuf)
//!
//! Data Model Mapping:
//! - Prometheus metric name → MynaDB measurement
//! - Prometheus labels → MynaDB tags
//! - Prometheus value → MynaDB "value" field
//! - Prometheus timestamp (ms) → MynaDB timestamp (ns)

pub mod remote_read;
pub mod remote_write;
mod types;

pub use remote_read::remote_read_handler;
pub use remote_write::remote_write_handler;
pub use types::*;
