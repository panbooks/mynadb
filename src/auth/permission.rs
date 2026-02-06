//! Permission definitions for RBAC

use serde::{Deserialize, Serialize};
use std::fmt;

/// Permissions that can be granted to users/roles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    /// Read data: SELECT queries, schema inspection, list tables/databases
    Read,
    /// Write data: INSERT via /write endpoint
    Write,
    /// Admin operations: DELETE partitions, compaction, retention, cache management, profiling
    Admin,
}

impl Permission {
    /// Get all possible permissions
    pub fn all() -> Vec<Permission> {
        vec![Permission::Read, Permission::Write, Permission::Admin]
    }

    /// Parse permission from string
    pub fn from_str(s: &str) -> Option<Permission> {
        match s.to_lowercase().as_str() {
            "read" => Some(Permission::Read),
            "write" => Some(Permission::Write),
            "admin" => Some(Permission::Admin),
            _ => None,
        }
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Permission::Read => write!(f, "read"),
            Permission::Write => write!(f, "write"),
            Permission::Admin => write!(f, "admin"),
        }
    }
}

/// Resource that permissions apply to
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    /// Global resource (system-wide operations)
    Global,
    /// Database-level resource
    Database(String),
    /// Table-level resource (database.schema.table)
    Table {
        database: String,
        schema: String,
        table: String,
    },
}

impl Resource {
    /// Check if this resource is contained within another resource
    ///
    /// - Global contains everything
    /// - Database contains all tables in that database
    /// - Table is the most specific
    pub fn is_contained_in(&self, other: &Resource) -> bool {
        match (self, other) {
            // Global contains everything
            (_, Resource::Global) => true,
            // Database contains all tables in that database
            (Resource::Table { database, .. }, Resource::Database(db)) => database == db,
            // Same database
            (Resource::Database(a), Resource::Database(b)) => a == b,
            // Same table
            (
                Resource::Table {
                    database: d1,
                    schema: s1,
                    table: t1,
                },
                Resource::Table {
                    database: d2,
                    schema: s2,
                    table: t2,
                },
            ) => d1 == d2 && s1 == s2 && t1 == t2,
            // Other combinations don't match
            _ => false,
        }
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resource::Global => write!(f, "*"),
            Resource::Database(db) => write!(f, "{}.*", db),
            Resource::Table {
                database,
                schema,
                table,
            } => write!(f, "{}.{}.{}", database, schema, table),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_from_str() {
        assert_eq!(Permission::from_str("read"), Some(Permission::Read));
        assert_eq!(Permission::from_str("WRITE"), Some(Permission::Write));
        assert_eq!(Permission::from_str("Admin"), Some(Permission::Admin));
        assert_eq!(Permission::from_str("invalid"), None);
    }

    #[test]
    fn test_resource_containment() {
        let global = Resource::Global;
        let db = Resource::Database("mydb".to_string());
        let table = Resource::Table {
            database: "mydb".to_string(),
            schema: "default".to_string(),
            table: "cpu".to_string(),
        };
        let other_db = Resource::Database("other".to_string());

        // Global contains everything
        assert!(db.is_contained_in(&global));
        assert!(table.is_contained_in(&global));

        // Database contains its tables
        assert!(table.is_contained_in(&db));
        assert!(!table.is_contained_in(&other_db));

        // Database doesn't contain other databases
        assert!(!other_db.is_contained_in(&db));
    }
}
