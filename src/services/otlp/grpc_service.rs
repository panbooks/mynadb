//! OTLP gRPC Service Implementation
//!
//! Implements the OpenTelemetry Collector gRPC services:
//! - MetricsService: receives metrics via gRPC on port 4317
//! - LogsService: receives logs via gRPC on port 4317
//! - TraceService: receives traces via gRPC on port 4317

use super::{logs_converter, metrics_converter, traces_converter};
use crate::config::OtlpConfig;
use crate::storage::cache::DataCache;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, warn};

/// Shared state for OTLP gRPC services
pub struct OtlpGrpcState {
    pub cache: DataCache,
    pub config: OtlpConfig,
}

/// OTLP Metrics gRPC service
pub struct OtlpMetricsService {
    state: Arc<OtlpGrpcState>,
}

impl OtlpMetricsService {
    pub fn new(state: Arc<OtlpGrpcState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService
    for OtlpMetricsService
{
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();
        let points_by_table = metrics_converter::convert_metrics(
            &req,
            &self.state.config.database,
            &self.state.config.schema,
            self.state.config.resource_as_tags,
            self.state.config.flatten_attributes,
        );

        let mut total_points = 0;
        for (table, points) in points_by_table {
            total_points += points.len();
            if let Err(e) = self
                .state
                .cache
                .add_points(
                    &self.state.config.database,
                    &self.state.config.schema,
                    &table,
                    points,
                )
                .await
            {
                error!("OTLP metrics write failed for {}: {:?}", table, e);
                return Err(Status::internal(format!("Write failed: {:?}", e)));
            }
        }

        debug!("OTLP gRPC: ingested {} metric points", total_points);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

/// OTLP Logs gRPC service
pub struct OtlpLogsService {
    state: Arc<OtlpGrpcState>,
}

impl OtlpLogsService {
    pub fn new(state: Arc<OtlpGrpcState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService
    for OtlpLogsService
{
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        let points = logs_converter::convert_logs(
            &req,
            &self.state.config.schema,
            self.state.config.resource_as_tags,
            self.state.config.flatten_attributes,
        );

        let count = points.len();
        if !points.is_empty() {
            if let Err(e) = self
                .state
                .cache
                .add_points(
                    &self.state.config.database,
                    &self.state.config.schema,
                    logs_converter::LOGS_TABLE,
                    points,
                )
                .await
            {
                error!("OTLP logs write failed: {:?}", e);
                return Err(Status::internal(format!("Write failed: {:?}", e)));
            }
        }

        debug!("OTLP gRPC: ingested {} log records", count);
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

/// OTLP Trace gRPC service
pub struct OtlpTraceService {
    state: Arc<OtlpGrpcState>,
}

impl OtlpTraceService {
    pub fn new(state: Arc<OtlpGrpcState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService
    for OtlpTraceService
{
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();
        let result = traces_converter::convert_traces(
            &req,
            &self.state.config.schema,
            self.state.config.resource_as_tags,
            self.state.config.flatten_attributes,
        );

        let total = result.spans.len() + result.events.len() + result.links.len();
        let db = &self.state.config.database;
        let schema = &self.state.config.schema;

        // Write spans
        if !result.spans.is_empty() {
            if let Err(e) = self
                .state
                .cache
                .add_points(db, schema, traces_converter::TRACES_TABLE, result.spans)
                .await
            {
                error!("OTLP traces write failed: {:?}", e);
                return Err(Status::internal(format!("Write failed: {:?}", e)));
            }
        }

        // Write span events
        if !result.events.is_empty() {
            if let Err(e) = self
                .state
                .cache
                .add_points(
                    db,
                    schema,
                    traces_converter::TRACE_EVENTS_TABLE,
                    result.events,
                )
                .await
            {
                warn!("OTLP trace events write failed: {:?}", e);
            }
        }

        // Write span links
        if !result.links.is_empty() {
            if let Err(e) = self
                .state
                .cache
                .add_points(
                    db,
                    schema,
                    traces_converter::TRACE_LINKS_TABLE,
                    result.links,
                )
                .await
            {
                warn!("OTLP trace links write failed: {:?}", e);
            }
        }

        debug!("OTLP gRPC: ingested {} trace points", total);
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}
