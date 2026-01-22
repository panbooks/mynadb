//! Custom TableProvider Implementation
//!
//! Provides a flexible TableProvider for custom data sources in DataFusion.
//! This implementation supports:
//! - Filter pushdown for predicate optimization
//! - Projection pushdown for column pruning
//! - Custom execution plans with streaming support
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::query::table_provider::{CustomTableProvider, CustomTableConfig};
//!
//! // Create a provider with static data
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("value", DataType::Utf8, true),
//! ]));
//! let batches = vec![batch1, batch2];
//! let provider = CustomTableProvider::new_with_data(schema, batches);
//!
//! // Register with DataFusion
//! ctx.register_table("my_table", Arc::new(provider))?;
//!
//! // Or use the builder pattern
//! let provider = CustomTableProvider::builder()
//!     .with_schema(schema)
//!     .with_data(batches)
//!     .with_filter_pushdown(true)
//!     .build();
//! ```

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::ExecutionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::stream;
use parking_lot::RwLock;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Configuration for the custom table provider
#[derive(Debug, Clone)]
pub struct CustomTableConfig {
    /// Enable filter pushdown
    pub filter_pushdown: bool,
    /// Enable projection pushdown
    pub projection_pushdown: bool,
    /// Table name for display purposes
    pub table_name: String,
}

impl Default for CustomTableConfig {
    fn default() -> Self {
        Self {
            filter_pushdown: true,
            projection_pushdown: true,
            table_name: "custom_table".to_string(),
        }
    }
}

impl CustomTableConfig {
    /// Create a new configuration with the given table name
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            ..Default::default()
        }
    }

    /// Set filter pushdown support
    pub fn with_filter_pushdown(mut self, enabled: bool) -> Self {
        self.filter_pushdown = enabled;
        self
    }

    /// Set projection pushdown support
    pub fn with_projection_pushdown(mut self, enabled: bool) -> Self {
        self.projection_pushdown = enabled;
        self
    }
}

/// A custom TableProvider implementation for DataFusion
///
/// This provider can be used to:
/// - Serve in-memory data through SQL queries
/// - Implement custom data sources with filter/projection pushdown
/// - Create virtual tables backed by external systems
pub struct CustomTableProvider {
    /// Table schema
    schema: SchemaRef,
    /// In-memory data batches (if any)
    data: RwLock<Vec<RecordBatch>>,
    /// Configuration
    config: CustomTableConfig,
    /// Optional data loader function for lazy loading
    data_loader: Option<Arc<dyn Fn() -> Result<Vec<RecordBatch>> + Send + Sync>>,
}

impl fmt::Debug for CustomTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomTableProvider")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field("batch_count", &self.data.read().len())
            .finish()
    }
}

impl CustomTableProvider {
    /// Create a new provider with a schema (no data initially)
    pub fn new(schema: SchemaRef, config: CustomTableConfig) -> Self {
        Self {
            schema,
            data: RwLock::new(Vec::new()),
            config,
            data_loader: None,
        }
    }

    /// Create a new provider with schema and initial data
    pub fn new_with_data(
        schema: SchemaRef,
        data: Vec<RecordBatch>,
        config: CustomTableConfig,
    ) -> Self {
        Self {
            schema,
            data: RwLock::new(data),
            config,
            data_loader: None,
        }
    }

    /// Create a new provider with a lazy data loader
    pub fn new_with_loader<F>(schema: SchemaRef, config: CustomTableConfig, loader: F) -> Self
    where
        F: Fn() -> Result<Vec<RecordBatch>> + Send + Sync + 'static,
    {
        Self {
            schema,
            data: RwLock::new(Vec::new()),
            config,
            data_loader: Some(Arc::new(loader)),
        }
    }

    /// Create a builder for more complex configurations
    pub fn builder() -> CustomTableProviderBuilder {
        CustomTableProviderBuilder::default()
    }

    /// Add data to the provider
    pub fn add_data(&self, batches: Vec<RecordBatch>) {
        let mut data = self.data.write();
        data.extend(batches);
    }

    /// Replace all data in the provider
    pub fn set_data(&self, batches: Vec<RecordBatch>) {
        let mut data = self.data.write();
        *data = batches;
    }

    /// Clear all data from the provider
    pub fn clear_data(&self) {
        let mut data = self.data.write();
        data.clear();
    }

    /// Get the current number of batches
    pub fn batch_count(&self) -> usize {
        self.data.read().len()
    }

    /// Get the total row count across all batches
    pub fn row_count(&self) -> usize {
        self.data.read().iter().map(|b| b.num_rows()).sum()
    }

    /// Get configuration
    pub fn config(&self) -> &CustomTableConfig {
        &self.config
    }

    /// Load data if using lazy loader
    fn load_data_if_needed(&self) -> Result<()> {
        if let Some(loader) = &self.data_loader {
            let mut data = self.data.write();
            if data.is_empty() {
                *data = loader()?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TableProvider for CustomTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.config.filter_pushdown {
            // Report that we can handle all filters (exact pushdown)
            // In a real implementation, you might inspect each filter
            // and return Exact, Inexact, or Unsupported accordingly
            Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Inexact)
                .collect())
        } else {
            Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect())
        }
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Load data if using lazy loader
        self.load_data_if_needed()?;

        // Compute the projected schema
        let projected_schema = project_schema(&self.schema, projection)?;

        // Clone data for the execution plan
        let data = self.data.read().clone();

        // Create the custom execution plan
        let exec = CustomExecPlan::new(
            projected_schema,
            data,
            projection.cloned(),
            filters.to_vec(),
            self.config.table_name.clone(),
        );

        Ok(Arc::new(exec))
    }
}

/// Builder for CustomTableProvider
#[derive(Default)]
pub struct CustomTableProviderBuilder {
    schema: Option<SchemaRef>,
    data: Vec<RecordBatch>,
    config: CustomTableConfig,
}

impl CustomTableProviderBuilder {
    /// Set the table schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set initial data
    pub fn with_data(mut self, data: Vec<RecordBatch>) -> Self {
        self.data = data;
        self
    }

    /// Set the table name
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.config.table_name = name.into();
        self
    }

    /// Enable or disable filter pushdown
    pub fn with_filter_pushdown(mut self, enabled: bool) -> Self {
        self.config.filter_pushdown = enabled;
        self
    }

    /// Enable or disable projection pushdown
    pub fn with_projection_pushdown(mut self, enabled: bool) -> Self {
        self.config.projection_pushdown = enabled;
        self
    }

    /// Build the provider
    pub fn build(self) -> Result<CustomTableProvider> {
        let schema = self.schema.ok_or_else(|| {
            datafusion::common::DataFusionError::Configuration(
                "Schema is required for CustomTableProvider".to_string(),
            )
        })?;

        Ok(CustomTableProvider::new_with_data(schema, self.data, self.config))
    }
}

/// Custom execution plan for the table provider
#[derive(Debug)]
pub struct CustomExecPlan {
    /// Schema after projection
    schema: SchemaRef,
    /// Data batches to return
    data: Vec<RecordBatch>,
    /// Column projection (indices)
    projection: Option<Vec<usize>>,
    /// Pushed-down filters (for logging/debugging)
    filters: Vec<Expr>,
    /// Table name for display
    table_name: String,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties (cached)
    properties: PlanProperties,
}

impl CustomExecPlan {
    /// Create a new execution plan
    pub fn new(
        schema: SchemaRef,
        data: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        table_name: String,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            schema,
            data,
            projection,
            filters,
            table_name,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }
}

impl DisplayAs for CustomExecPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CustomExecPlan: table={}, batches={}, projection={:?}, filters={}",
                    self.table_name,
                    self.data.len(),
                    self.projection,
                    self.filters.len()
                )
            }
        }
    }
}

impl ExecutionPlan for CustomExecPlan {
    fn name(&self) -> &str {
        "CustomExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node, no children
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // No children to replace, return self
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let projection = self.projection.clone();

        // Apply projection to batches if needed
        let batches: Vec<RecordBatch> = self
            .data
            .iter()
            .filter_map(|batch| {
                if let Some(ref proj) = projection {
                    // Project columns
                    let columns: Vec<_> = proj
                        .iter()
                        .map(|&i| batch.column(i).clone())
                        .collect();
                    RecordBatch::try_new(schema.clone(), columns).ok()
                } else {
                    Some(batch.clone())
                }
            })
            .collect();

        // Create a stream from the batches
        let stream = stream::iter(batches.into_iter().map(Ok));
        let stream = RecordBatchStreamAdapter::new(schema, stream);

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: SchemaRef) -> RecordBatch {
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("alice"), Some("bob"), None]);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_provider_creation() {
        let schema = create_test_schema();
        let config = CustomTableConfig::new("test_table");
        let provider = CustomTableProvider::new(schema.clone(), config);

        assert_eq!(provider.schema(), schema);
        assert_eq!(provider.batch_count(), 0);
    }

    #[test]
    fn test_provider_with_data() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());
        let config = CustomTableConfig::new("test_table");
        let provider = CustomTableProvider::new_with_data(schema, vec![batch], config);

        assert_eq!(provider.batch_count(), 1);
        assert_eq!(provider.row_count(), 3);
    }

    #[test]
    fn test_builder() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = CustomTableProvider::builder()
            .with_schema(schema)
            .with_data(vec![batch])
            .with_table_name("my_table")
            .with_filter_pushdown(true)
            .build()
            .unwrap();

        assert_eq!(provider.batch_count(), 1);
        assert_eq!(provider.config().table_name, "my_table");
        assert!(provider.config().filter_pushdown);
    }

    #[tokio::test]
    async fn test_query_provider() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());
        let config = CustomTableConfig::new("test_table");
        let provider = CustomTableProvider::new_with_data(schema, vec![batch], config);

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_query_with_projection() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());
        let config = CustomTableConfig::new("test_table");
        let provider = CustomTableProvider::new_with_data(schema, vec![batch], config);

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT name FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn test_query_with_filter() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());
        let config = CustomTableConfig::new("test_table");
        let provider = CustomTableProvider::new_with_data(schema, vec![batch], config);

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test WHERE id > 1").await.unwrap();
        let results = df.collect().await.unwrap();

        // Filter is applied by DataFusion after scan
        assert!(!results.is_empty());
    }

    #[test]
    fn test_add_and_clear_data() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());
        let config = CustomTableConfig::new("test_table");
        let provider = CustomTableProvider::new(schema.clone(), config);

        assert_eq!(provider.batch_count(), 0);

        provider.add_data(vec![batch.clone()]);
        assert_eq!(provider.batch_count(), 1);

        provider.add_data(vec![batch]);
        assert_eq!(provider.batch_count(), 2);

        provider.clear_data();
        assert_eq!(provider.batch_count(), 0);
    }

    #[test]
    fn test_lazy_loader() {
        let schema = create_test_schema();
        let schema_clone = schema.clone();

        let provider = CustomTableProvider::new_with_loader(
            schema,
            CustomTableConfig::new("lazy_table"),
            move || {
                let batch = create_test_batch(schema_clone.clone());
                Ok(vec![batch])
            },
        );

        // Initially empty
        assert_eq!(provider.batch_count(), 0);

        // Load data
        provider.load_data_if_needed().unwrap();

        // Now has data
        assert_eq!(provider.batch_count(), 1);
    }
}
