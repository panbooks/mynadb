//! Iceberg REST Catalog API
//!
//! Implements the Iceberg REST Catalog specification for external
//! query engines (Spark, Trino, DuckDB, Flink) to discover and
//! read MynaDB tables as Iceberg tables.
//!
//! Spec: https://iceberg.apache.org/spec/#rest-catalog

use super::mynadb_catalog::MynaDbIcebergCatalog;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Shared state for Iceberg REST handlers
pub struct IcebergRestState {
    pub catalog: Arc<MynaDbIcebergCatalog>,
    /// Default catalog name (warehouse)
    pub default_catalog: String,
}

// ============================================================================
// Response types per Iceberg REST spec
// ============================================================================

#[derive(Serialize)]
struct ConfigResponse {
    defaults: HashMap<String, String>,
    overrides: HashMap<String, String>,
}

#[derive(Serialize)]
struct ListNamespacesResponse {
    namespaces: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct NamespaceResponse {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Serialize)]
struct ListTablesResponse {
    identifiers: Vec<TableIdentifier>,
}

#[derive(Serialize)]
struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

#[derive(Serialize)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    code: u16,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /iceberg/v1/config
pub async fn config_handler(
    State(state): State<Arc<IcebergRestState>>,
) -> impl IntoResponse {
    let mut defaults = HashMap::new();
    defaults.insert("warehouse".to_string(), state.default_catalog.clone());

    Json(ConfigResponse {
        defaults,
        overrides: HashMap::new(),
    })
}

/// GET /iceberg/v1/namespaces
pub async fn list_namespaces_handler(
    State(state): State<Arc<IcebergRestState>>,
) -> impl IntoResponse {
    let namespaces = state
        .catalog
        .list_namespaces(&state.default_catalog)
        .into_iter()
        .map(|ns| vec![ns])
        .collect();

    Json(ListNamespacesResponse { namespaces })
}

/// GET /iceberg/v1/namespaces/:namespace
pub async fn get_namespace_handler(
    State(state): State<Arc<IcebergRestState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let namespaces = state.catalog.list_namespaces(&state.default_catalog);
    if !namespaces.contains(&namespace) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": {
                    "message": format!("Namespace '{}' not found", namespace),
                    "type": "NoSuchNamespaceException",
                    "code": 404
                }
            })),
        )
            .into_response();
    }

    let mut properties = HashMap::new();
    properties.insert("owner".to_string(), "mynadb".to_string());

    Json(NamespaceResponse {
        namespace: vec![namespace],
        properties,
    })
    .into_response()
}

/// GET /iceberg/v1/namespaces/:namespace/tables
pub async fn list_tables_handler(
    State(state): State<Arc<IcebergRestState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let tables = state
        .catalog
        .list_tables(&state.default_catalog, &namespace);

    let identifiers = tables
        .into_iter()
        .map(|name| TableIdentifier {
            namespace: vec![namespace.clone()],
            name,
        })
        .collect();

    Json(ListTablesResponse { identifiers })
}

/// GET /iceberg/v1/namespaces/:namespace/tables/:table
pub async fn load_table_handler(
    State(state): State<Arc<IcebergRestState>>,
    Path((namespace, table)): Path<(String, String)>,
) -> impl IntoResponse {
    match state
        .catalog
        .get_table_metadata(&state.default_catalog, &namespace, &table)
    {
        Some(metadata) => {
            let metadata_location = format!(
                "{}/iceberg/{}/{}/{}/metadata/v{}.metadata.json",
                state.default_catalog,
                state.default_catalog,
                namespace,
                table,
                metadata.last_sequence_number.max(1)
            );

            let metadata_value = serde_json::to_value(&metadata).unwrap_or_default();

            Json(LoadTableResponse {
                metadata_location,
                metadata: metadata_value,
            })
            .into_response()
        }
        None => {
            if !state
                .catalog
                .table_exists(&state.default_catalog, &namespace, &table)
            {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": {
                            "message": format!("Table '{}.{}' not found", namespace, table),
                            "type": "NoSuchTableException",
                            "code": 404
                        }
                    })),
                )
                    .into_response()
            } else {
                // Table exists in MynaDB but no Iceberg metadata yet
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": {
                            "message": format!("Table '{}.{}' exists but Iceberg metadata not yet generated. Wait for sync.", namespace, table),
                            "type": "NoSuchTableException",
                            "code": 404
                        }
                    })),
                )
                    .into_response()
            }
        }
    }
}

/// HEAD /iceberg/v1/namespaces/:namespace/tables/:table
pub async fn table_exists_handler(
    State(state): State<Arc<IcebergRestState>>,
    Path((namespace, table)): Path<(String, String)>,
) -> impl IntoResponse {
    if state
        .catalog
        .table_exists(&state.default_catalog, &namespace, &table)
    {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

/// Build the Iceberg REST Catalog router
pub fn build_iceberg_router(state: Arc<IcebergRestState>) -> axum::Router<()> {
    use axum::routing::{get, head};

    axum::Router::new()
        .route("/v1/config", get(config_handler))
        .route("/v1/namespaces", get(list_namespaces_handler))
        .route("/v1/namespaces/:namespace", get(get_namespace_handler))
        .route(
            "/v1/namespaces/:namespace/tables",
            get(list_tables_handler),
        )
        .route(
            "/v1/namespaces/:namespace/tables/:table",
            get(load_table_handler),
        )
        .route(
            "/v1/namespaces/:namespace/tables/:table",
            head(table_exists_handler),
        )
        .with_state(state)
}
