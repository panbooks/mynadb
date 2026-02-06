use crate::observability::config::TracingConfig;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, Tracer};
use opentelemetry_sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};
use tracing_opentelemetry::OpenTelemetryLayer;
use std::error::Error;

/// Initialize OpenTelemetry tracing
pub fn init_tracing(config: &TracingConfig) -> Result<Option<Tracer>, Box<dyn Error + Send + Sync>> {
    if !config.enabled {
        return Ok(None);
    }

    let tracer = if let Some(ref endpoint) = config.otlp_endpoint {
        // Initialize OpenTelemetry OTLP exporter
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
            .build_span_exporter()?;

        let trace_config = opentelemetry_sdk::trace::Config::default()
            .with_resource(Resource::new(vec![
                KeyValue::new("service.name", config.service_name.clone()),
            ]))
            .with_id_generator(RandomIdGenerator::default())
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                config.sample_rate,
            ))));

        let provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_config(trace_config)
            .build();

        let tracer = provider.tracer(config.service_name.clone());

        // Set up tracing subscriber with OpenTelemetry layer
        let telemetry_layer = OpenTelemetryLayer::new(tracer.clone());

        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        Registry::default()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .with(telemetry_layer)
            .try_init()?;

        Some(tracer)
    } else {
        // No OTLP endpoint, just use regular tracing
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        Registry::default()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;

        None
    };

    tracing::info!(
        enabled = config.enabled,
        otlp_endpoint = ?config.otlp_endpoint,
        service_name = %config.service_name,
        sample_rate = config.sample_rate,
        "OpenTelemetry tracing initialized"
    );

    Ok(tracer)
}

/// Shutdown OpenTelemetry gracefully
pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}
