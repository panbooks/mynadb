//! OpenTelemetry Protocol (OTLP) Native Ingestion
//!
//! Receives metrics, logs, and traces via OTLP gRPC (port 4317) and HTTP
//! endpoints, converts them to MynaDB's internal Point format, and writes
//! them through the standard DataCache write path.
//!
//! ## Data Flow
//!
//! ```text
//! OTLP gRPC (port 4317)         OTLP HTTP (/v1/* on main port)
//!          |                              |
//!          v                              v
//!   tonic gRPC handlers           Axum HTTP handlers
//!          |                              |
//!          +----------> OTLP Converters <-+
//!                       (metrics/logs/traces -> Vec<Point>)
//!                             |
//!                             v
//!                   DataCache.add_points()
//!                   (existing write path)
//! ```

pub mod converter;
pub mod grpc_service;
pub mod http_handlers;
pub mod logs_converter;
pub mod metrics_converter;
pub mod traces_converter;
