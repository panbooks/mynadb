//! Arrow <-> Iceberg Schema Conversion
//!
//! Converts Arrow schemas (from Parquet files) to Iceberg v2 schema format.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Iceberg v2 Schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSchema {
    #[serde(rename = "type")]
    pub schema_type: String, // always "struct"
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub fields: Vec<IcebergField>,
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub identifier_field_ids: Option<Vec<i32>>,
}

/// Iceberg v2 Field (nested type support)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: IcebergType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// Iceberg type representation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IcebergType {
    Primitive(String),
    Struct {
        #[serde(rename = "type")]
        struct_type: String,
        fields: Vec<IcebergField>,
    },
    List {
        #[serde(rename = "type")]
        list_type: String,
        #[serde(rename = "element-id")]
        element_id: i32,
        element: Box<IcebergType>,
        #[serde(rename = "element-required")]
        element_required: bool,
    },
    Map {
        #[serde(rename = "type")]
        map_type: String,
        #[serde(rename = "key-id")]
        key_id: i32,
        key: Box<IcebergType>,
        #[serde(rename = "value-id")]
        value_id: i32,
        value: Box<IcebergType>,
        #[serde(rename = "value-required")]
        value_required: bool,
    },
}

/// Convert an Arrow schema to an Iceberg v2 schema
pub fn arrow_to_iceberg_schema(
    arrow_schema: &arrow::datatypes::Schema,
    schema_id: i32,
) -> IcebergSchema {
    let mut fields = Vec::new();
    let mut field_id = 1;

    for arrow_field in arrow_schema.fields() {
        let iceberg_type = arrow_type_to_iceberg(arrow_field.data_type());
        fields.push(IcebergField {
            id: field_id,
            name: arrow_field.name().clone(),
            required: !arrow_field.is_nullable(),
            field_type: iceberg_type,
            doc: None,
        });
        field_id += 1;
    }

    IcebergSchema {
        schema_type: "struct".to_string(),
        schema_id,
        fields,
        identifier_field_ids: None,
    }
}

/// Convert an Arrow DataType to an Iceberg type string
fn arrow_type_to_iceberg(arrow_type: &arrow::datatypes::DataType) -> IcebergType {
    use arrow::datatypes::DataType;

    let primitive = match arrow_type {
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "int",
        DataType::Int64 => "long",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "int",
        DataType::UInt64 => "long",
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binary",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::Timestamp(unit, _) => {
            match unit {
                arrow::datatypes::TimeUnit::Microsecond => "timestamptz",
                _ => "timestamptz",
            }
        }
        DataType::Duration(_) => "long",
        DataType::Decimal128(p, s) => {
            return IcebergType::Primitive(format!("decimal({},{})", p, s));
        }
        DataType::Decimal256(p, s) => {
            return IcebergType::Primitive(format!("decimal({},{})", p, s));
        }
        DataType::Dictionary(_, value_type) => {
            return arrow_type_to_iceberg(value_type);
        }
        _ => "binary", // fallback
    };

    IcebergType::Primitive(primitive.to_string())
}

/// Get the field ID for the "time" column (used for partition spec)
pub fn find_time_field_id(schema: &IcebergSchema) -> Option<i32> {
    schema
        .fields
        .iter()
        .find(|f| f.name == "time")
        .map(|f| f.id)
}

/// Get the field ID for the "date" column
pub fn find_date_field_id(schema: &IcebergSchema) -> Option<i32> {
    schema
        .fields
        .iter()
        .find(|f| f.name == "date")
        .map(|f| f.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_arrow_to_iceberg_schema() {
        let arrow_schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema, 0);
        assert_eq!(iceberg_schema.schema_type, "struct");
        assert_eq!(iceberg_schema.fields.len(), 5);
        assert_eq!(iceberg_schema.fields[0].name, "time");
        assert!(matches!(
            &iceberg_schema.fields[0].field_type,
            IcebergType::Primitive(s) if s == "timestamptz"
        ));
        assert_eq!(iceberg_schema.fields[0].required, true);
    }

    #[test]
    fn test_find_time_field() {
        let schema = IcebergSchema {
            schema_type: "struct".to_string(),
            schema_id: 0,
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "time".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive("timestamptz".to_string()),
                    doc: None,
                },
                IcebergField {
                    id: 2,
                    name: "value".to_string(),
                    required: false,
                    field_type: IcebergType::Primitive("double".to_string()),
                    doc: None,
                },
            ],
            identifier_field_ids: None,
        };

        assert_eq!(find_time_field_id(&schema), Some(1));
        assert_eq!(find_date_field_id(&schema), None);
    }
}
