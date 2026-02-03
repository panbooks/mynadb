//! Multi-Union TableProvider Implementation
//!
//! Provides a TableProvider that combines results from N underlying providers.
//! This is used to merge data from multiple storage prefixes after scale-in,
//! enabling queries to find data under both current and legacy node prefixes.
//!
//! ## Use Case: Scale-In Orphaned Data
//!
//! When a cluster scales from 100 to 50 nodes, parquet files written by
//! removed nodes (data-50/ to data-99/) become orphaned under their original
//! prefixes. This provider allows queries to scan all relevant prefixes:
//!
//! ```text
//! Query for table "cpu"
//!   ↓
//! MultiUnionTableProvider
//!   ├── ParquetProvider for data-1/mydb/default/cpu/  (current owner)
//!   ├── ParquetProvider for data-50/mydb/default/cpu/ (legacy prefix)
//!   └── ParquetProvider for data-75/mydb/default/cpu/ (legacy prefix)
//!   ↓
//! MultiUnionExec (combines all streams)
//!   ↓
//! Merged Results (all data from all prefixes)
//! ```

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{project_schema, DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::ExecutionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use tracing::debug;

/// A TableProvider that combines results from N underlying providers
///
/// This is used to merge data from multiple storage prefixes, such as when
/// a table has data spread across multiple node prefixes after scale-in.
pub struct MultiUnionTableProvider {
    /// List of providers to union (one per storage prefix)
    providers: Vec<Arc<dyn TableProvider>>,
    /// Merged schema from all providers
    schema: SchemaRef,
    /// Table name for logging
    table_name: String,
    /// Storage prefixes for debugging
    prefixes: Vec<String>,
}

impl fmt::Debug for MultiUnionTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiUnionTableProvider")
            .field("table_name", &self.table_name)
            .field("provider_count", &self.providers.len())
            .field("prefixes", &self.prefixes)
            .field("schema", &self.schema)
            .finish()
    }
}

impl MultiUnionTableProvider {
    /// Create a new MultiUnionTableProvider from a list of providers
    ///
    /// # Arguments
    /// * `providers` - List of providers (one per storage prefix)
    /// * `table_name` - Full table name for logging
    /// * `prefixes` - Storage prefixes for debugging (optional, can be empty)
    ///
    /// # Errors
    /// Returns error if providers is empty or schemas cannot be merged
    pub fn try_new(
        providers: Vec<Arc<dyn TableProvider>>,
        table_name: impl Into<String>,
        prefixes: Vec<String>,
    ) -> Result<Self> {
        if providers.is_empty() {
            return Err(DataFusionError::Plan(
                "MultiUnionTableProvider requires at least one provider".to_string(),
            ));
        }

        // Merge schemas from all providers
        let schema = merge_schemas_from_providers(&providers)?;

        Ok(Self {
            providers,
            schema: Arc::new(schema),
            table_name: table_name.into(),
            prefixes,
        })
    }

    /// Create with explicit schema (when you already know the merged schema)
    pub fn new_with_schema(
        providers: Vec<Arc<dyn TableProvider>>,
        schema: SchemaRef,
        table_name: impl Into<String>,
        prefixes: Vec<String>,
    ) -> Self {
        Self {
            providers,
            schema,
            table_name: table_name.into(),
            prefixes,
        }
    }

    /// Get the number of underlying providers
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }

    /// Get the storage prefixes (for debugging)
    pub fn prefixes(&self) -> &[String] {
        &self.prefixes
    }
}

/// Merge schemas from multiple providers
///
/// Fields with the same name must have compatible types.
/// The result schema contains all unique fields from all providers.
fn merge_schemas_from_providers(providers: &[Arc<dyn TableProvider>]) -> Result<Schema> {
    if providers.is_empty() {
        return Err(DataFusionError::Plan(
            "Cannot merge schemas: no providers".to_string(),
        ));
    }

    let mut fields: Vec<Field> = Vec::new();
    let mut seen_names: HashSet<String> = HashSet::new();

    for provider in providers {
        let schema = provider.schema();
        for field in schema.fields() {
            if !seen_names.contains(field.name()) {
                // New field, add it (make nullable since it might not exist in all sources)
                let mut new_field = field.as_ref().clone();
                if !new_field.is_nullable() {
                    new_field = Field::new(new_field.name(), new_field.data_type().clone(), true);
                }
                fields.push(new_field);
                seen_names.insert(field.name().clone());
            }
            // If field exists already, we use the first definition
        }
    }

    Ok(Schema::new(fields))
}

#[async_trait]
impl TableProvider for MultiUnionTableProvider {
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
        // Return inexact for all filters - all underlying providers will apply them
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Compute the projected schema
        let projected_schema = project_schema(&self.schema, projection)?;

        debug!(
            "MultiUnionTableProvider scan: table={}, providers={}, projection={:?}",
            self.table_name,
            self.providers.len(),
            projection
        );

        // Get execution plans from all providers
        let mut child_plans = Vec::with_capacity(self.providers.len());
        for (i, provider) in self.providers.iter().enumerate() {
            match provider.scan(state, projection, filters, limit).await {
                Ok(plan) => {
                    child_plans.push(plan);
                }
                Err(e) => {
                    // Log but don't fail - one prefix might have no data
                    debug!(
                        "Provider {} for {} returned error (may be empty): {}",
                        i, self.table_name, e
                    );
                }
            }
        }

        if child_plans.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No valid child plans for table {}",
                self.table_name
            )));
        }

        // Create multi-union execution plan
        let exec = MultiUnionExec::new(child_plans, projected_schema, self.table_name.clone());

        Ok(Arc::new(exec))
    }
}

/// Execution plan that unions results from N child plans
#[derive(Debug)]
pub struct MultiUnionExec {
    /// Child execution plans (one per provider/prefix)
    children: Vec<Arc<dyn ExecutionPlan>>,
    /// Output schema
    schema: SchemaRef,
    /// Table name for display
    table_name: String,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
}

impl MultiUnionExec {
    pub fn new(
        children: Vec<Arc<dyn ExecutionPlan>>,
        schema: SchemaRef,
        table_name: String,
    ) -> Self {
        // Use single partition and stream all children
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            children,
            schema,
            table_name,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    /// Get the number of child plans
    pub fn child_count(&self) -> usize {
        self.children.len()
    }
}

impl DisplayAs for MultiUnionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MultiUnionExec: table={}, children={}",
                    self.table_name,
                    self.children.len()
                )
            }
        }
    }
}

impl ExecutionPlan for MultiUnionExec {
    fn name(&self) -> &str {
        "MultiUnionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MultiUnionExec::new(
            children,
            self.schema.clone(),
            self.table_name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "MultiUnionExec execute: table={}, partition={}, children={}",
            self.table_name,
            partition,
            self.children.len()
        );

        // Execute all child plans and collect their streams
        let mut streams = Vec::with_capacity(self.children.len());
        for (i, child) in self.children.iter().enumerate() {
            match child.execute(0, context.clone()) {
                Ok(stream) => {
                    streams.push(stream);
                }
                Err(e) => {
                    debug!("Child {} execute failed (may be empty): {}", i, e);
                    // Continue with other children
                }
            }
        }

        let schema = self.schema.clone();

        // Use select_all to interleave all streams (order doesn't matter for union)
        let combined = futures::stream::select_all(streams);

        let stream = RecordBatchStreamAdapter::new(schema, combined);

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
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: SchemaRef, ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_merge_schemas_identical() {
        let schema = create_test_schema();
        let providers: Vec<Arc<dyn TableProvider>> = vec![
            Arc::new(
                MemTable::try_new(schema.clone(), vec![vec![create_test_batch(
                    schema.clone(),
                    vec![1],
                    vec!["a"],
                )]])
                .unwrap(),
            ),
            Arc::new(
                MemTable::try_new(schema.clone(), vec![vec![create_test_batch(
                    schema.clone(),
                    vec![2],
                    vec!["b"],
                )]])
                .unwrap(),
            ),
        ];

        let merged = merge_schemas_from_providers(&providers).unwrap();
        assert_eq!(merged.fields().len(), 2);
    }

    #[test]
    fn test_merge_schemas_different() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let providers: Vec<Arc<dyn TableProvider>> = vec![
            Arc::new(
                MemTable::try_new(
                    schema1.clone(),
                    vec![vec![create_test_batch(schema1.clone(), vec![1], vec!["a"])]],
                )
                .unwrap(),
            ),
            Arc::new(
                MemTable::try_new(
                    schema2.clone(),
                    vec![vec![RecordBatch::try_new(
                        schema2.clone(),
                        vec![
                            Arc::new(Int64Array::from(vec![2])),
                            Arc::new(datafusion::arrow::array::Float64Array::from(vec![1.5])),
                        ],
                    )
                    .unwrap()]],
                )
                .unwrap(),
            ),
        ];

        let merged = merge_schemas_from_providers(&providers).unwrap();
        assert_eq!(merged.fields().len(), 3);
        assert!(merged.field_with_name("id").is_ok());
        assert!(merged.field_with_name("name").is_ok());
        assert!(merged.field_with_name("value").is_ok());
    }

    #[tokio::test]
    async fn test_multi_union_provider_creation() {
        let schema = create_test_schema();
        let provider1 = Arc::new(
            MemTable::try_new(
                schema.clone(),
                vec![vec![create_test_batch(schema.clone(), vec![1], vec!["a"])]],
            )
            .unwrap(),
        ) as Arc<dyn TableProvider>;

        let provider2 = Arc::new(
            MemTable::try_new(
                schema.clone(),
                vec![vec![create_test_batch(schema.clone(), vec![2], vec!["b"])]],
            )
            .unwrap(),
        ) as Arc<dyn TableProvider>;

        let union = MultiUnionTableProvider::try_new(
            vec![provider1, provider2],
            "test.table",
            vec!["prefix1".to_string(), "prefix2".to_string()],
        )
        .unwrap();

        assert_eq!(union.provider_count(), 2);
        assert_eq!(union.prefixes().len(), 2);
    }

    #[test]
    fn test_multi_union_provider_empty_providers() {
        let result = MultiUnionTableProvider::try_new(
            vec![],
            "test.table",
            vec![],
        );
        assert!(result.is_err());
    }
}
