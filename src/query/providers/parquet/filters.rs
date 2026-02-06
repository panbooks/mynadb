//! Filter extraction and partition pruning logic

use super::config::{ParquetFileInfo, ParquetTableConfig};
use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter};
use datafusion::prelude::Expr;

/// Partition filter condition
#[derive(Debug, Clone)]
pub enum PartitionFilter {
    Eq(String),
    Gt(String),
    GtEq(String),
    Lt(String),
    LtEq(String),
}

/// Extracted equality predicate for bloom filter optimization
#[derive(Debug, Clone)]
pub struct EqualityPredicate {
    /// Column name
    pub column: String,
    /// Value to match (for string columns)
    pub value: String,
}

/// Extract partition filter conditions from expressions
pub fn extract_partition_filters(partition_column: &str, filters: &[Expr]) -> Vec<PartitionFilter> {
    let mut partition_filters = Vec::new();

    for filter in filters {
        extract_partition_filter_from_expr(partition_column, filter, &mut partition_filters);
    }

    partition_filters
}

/// Recursively extract partition filters from an expression
fn extract_partition_filter_from_expr(
    partition_column: &str,
    expr: &Expr,
    filters: &mut Vec<PartitionFilter>,
) {
    if let Expr::BinaryExpr(binary) = expr {
        // Check if this is a comparison on the partition column
        if let Expr::Column(col) = binary.left.as_ref() {
            if col.name == partition_column {
                if let Expr::Literal(scalar) = binary.right.as_ref() {
                    if let Some(value) = scalar
                        .to_string()
                        .strip_prefix("Utf8(\"")
                        .and_then(|s| s.strip_suffix("\")"))
                    {
                        let filter = match binary.op {
                            datafusion::logical_expr::Operator::Eq => {
                                PartitionFilter::Eq(value.to_string())
                            }
                            datafusion::logical_expr::Operator::Gt => {
                                PartitionFilter::Gt(value.to_string())
                            }
                            datafusion::logical_expr::Operator::GtEq => {
                                PartitionFilter::GtEq(value.to_string())
                            }
                            datafusion::logical_expr::Operator::Lt => {
                                PartitionFilter::Lt(value.to_string())
                            }
                            datafusion::logical_expr::Operator::LtEq => {
                                PartitionFilter::LtEq(value.to_string())
                            }
                            _ => return,
                        };
                        filters.push(filter);
                    }
                }
            }
        }

        // Recurse into AND expressions
        if binary.op == datafusion::logical_expr::Operator::And {
            extract_partition_filter_from_expr(partition_column, &binary.left, filters);
            extract_partition_filter_from_expr(partition_column, &binary.right, filters);
        }
    }
}

/// Check if a partition value matches the filter
pub fn matches_partition_filter(value: &str, filters: &[PartitionFilter]) -> bool {
    for filter in filters {
        let matches = match filter {
            PartitionFilter::Eq(v) => value == v,
            PartitionFilter::Gt(v) => value > v.as_str(),
            PartitionFilter::GtEq(v) => value >= v.as_str(),
            PartitionFilter::Lt(v) => value < v.as_str(),
            PartitionFilter::LtEq(v) => value <= v.as_str(),
        };
        if !matches {
            return false;
        }
    }
    true
}

/// Filter files based on partition predicates (both date and node_id)
pub fn filter_files_by_partition(
    files: &[ParquetFileInfo],
    config: &ParquetTableConfig,
    filters: &[Expr],
) -> Vec<ParquetFileInfo> {
    // Extract partition filter values for both date and node_id
    let date_filters = extract_partition_filters(&config.partition_column, filters);
    let node_id_filters = extract_partition_filters(&config.node_id_column, filters);

    if date_filters.is_empty() && node_id_filters.is_empty() {
        // No partition filters, return all files
        return files.to_vec();
    }

    // Filter files based on partition values
    files
        .iter()
        .filter(|f| {
            // Check date partition filter
            let date_matches = if date_filters.is_empty() {
                true
            } else if let Some(value) = f.partition_values.get(&config.partition_column) {
                matches_partition_filter(value, &date_filters)
            } else {
                true // Include files without partition values
            };

            // Check node_id partition filter
            let node_id_matches = if node_id_filters.is_empty() {
                true
            } else if let Some(value) = f.partition_values.get(&config.node_id_column) {
                matches_partition_filter(value, &node_id_filters)
            } else {
                true // Include files without partition values
            };

            date_matches && node_id_matches
        })
        .cloned()
        .collect()
}

/// Extract equality predicates from DataFusion expressions
/// These are the predicates that can benefit from bloom filter checking
pub fn extract_equality_predicates(filters: &[Expr], schema: &SchemaRef) -> Vec<EqualityPredicate> {
    let mut predicates = Vec::new();

    for filter in filters {
        extract_equality_from_expr(filter, schema, &mut predicates);
    }

    predicates
}

/// Recursively extract equality predicates from an expression
fn extract_equality_from_expr(
    expr: &Expr,
    schema: &SchemaRef,
    predicates: &mut Vec<EqualityPredicate>,
) {
    if let Expr::BinaryExpr(binary) = expr {
        // Handle equality: column = 'value'
        if binary.op == datafusion::logical_expr::Operator::Eq {
            if let (Expr::Column(col), Expr::Literal(scalar)) =
                (binary.left.as_ref(), binary.right.as_ref())
            {
                // Only extract for UTF8 columns (bloom filter targets)
                if let Ok(field) = schema.field_with_name(&col.name) {
                    if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                        if let Some(value) = scalar_to_string(scalar) {
                            predicates.push(EqualityPredicate {
                                column: col.name.clone(),
                                value,
                            });
                        }
                    }
                }
            }
            // Handle reversed: 'value' = column
            if let (Expr::Literal(scalar), Expr::Column(col)) =
                (binary.left.as_ref(), binary.right.as_ref())
            {
                if let Ok(field) = schema.field_with_name(&col.name) {
                    if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                        if let Some(value) = scalar_to_string(scalar) {
                            predicates.push(EqualityPredicate {
                                column: col.name.clone(),
                                value,
                            });
                        }
                    }
                }
            }
        }
        // Recurse into AND expressions
        if binary.op == datafusion::logical_expr::Operator::And {
            extract_equality_from_expr(&binary.left, schema, predicates);
            extract_equality_from_expr(&binary.right, schema, predicates);
        }
    }
}

/// Convert a scalar value to string
fn scalar_to_string(scalar: &datafusion::common::ScalarValue) -> Option<String> {
    match scalar {
        datafusion::common::ScalarValue::Utf8(Some(s)) => Some(s.clone()),
        datafusion::common::ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => {
            // Try to extract from the debug format for other representations
            let s = scalar.to_string();
            if let Some(value) = s
                .strip_prefix("Utf8(\"")
                .and_then(|s| s.strip_suffix("\")"))
            {
                return Some(value.to_string());
            }
            None
        }
    }
}

/// Create a row filter from equality predicates for Parquet reading
/// This enables row-level filtering with potential bloom filter benefits
pub fn create_row_filter_from_predicates(
    predicates: Vec<EqualityPredicate>,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Option<RowFilter> {
    if predicates.is_empty() {
        return None;
    }

    let mut arrow_predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    for pred in predicates {
        // Find the column index in the parquet schema
        let col_idx = (0..parquet_schema.num_columns())
            .find(|&i| parquet_schema.column(i).name() == pred.column);

        if let Some(idx) = col_idx {
            let projection =
                datafusion::parquet::arrow::ProjectionMask::roots(parquet_schema, vec![idx]);
            let expected_value = pred.value.clone();

            let predicate_fn = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
                if batch.num_columns() == 0 {
                    return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
                }

                let col = batch.column(0);

                // Handle different string array types
                if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
                    let result: BooleanArray = string_array
                        .iter()
                        .map(|opt_val| opt_val.map(|v| v == expected_value))
                        .collect();
                    Ok(result)
                } else if let Some(string_view_array) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                {
                    let result: BooleanArray = string_view_array
                        .iter()
                        .map(|opt_val| opt_val.map(|v| v == expected_value))
                        .collect();
                    Ok(result)
                } else if let Some(large_string_array) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                {
                    let result: BooleanArray = large_string_array
                        .iter()
                        .map(|opt_val| opt_val.map(|v| v == expected_value))
                        .collect();
                    Ok(result)
                } else {
                    // Column type doesn't match, return all true (don't filter)
                    Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                }
            });

            arrow_predicates.push(Box::new(predicate_fn));
        }
    }

    if arrow_predicates.is_empty() {
        return None;
    }

    Some(RowFilter::new(arrow_predicates))
}
