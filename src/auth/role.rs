//! Role definitions for RBAC

use super::Permission;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A role defines a set of permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Permissions granted by this role
    pub permissions: HashSet<Permission>,
}

impl Role {
    /// Create a new role with given permissions
    pub fn new(name: &str, permissions: Vec<Permission>) -> Self {
        Self {
            name: name.to_string(),
            permissions: permissions.into_iter().collect(),
        }
    }

    /// Check if this role has a specific permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.permissions.contains(&permission)
    }

    /// Get built-in admin role (all permissions)
    pub fn admin() -> Self {
        Self::new("admin", Permission::all())
    }

    /// Get built-in reader role (read-only)
    pub fn reader() -> Self {
        Self::new("reader", vec![Permission::Read])
    }

    /// Get built-in writer role (read + write)
    pub fn writer() -> Self {
        Self::new("writer", vec![Permission::Read, Permission::Write])
    }

    /// Get a built-in role by name
    pub fn builtin(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "admin" => Some(Self::admin()),
            "reader" => Some(Self::reader()),
            "writer" => Some(Self::writer()),
            _ => None,
        }
    }

    /// Get all built-in role names
    pub fn builtin_names() -> Vec<&'static str> {
        vec!["admin", "reader", "writer"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_role() {
        let admin = Role::admin();
        assert!(admin.has_permission(Permission::Read));
        assert!(admin.has_permission(Permission::Write));
        assert!(admin.has_permission(Permission::Admin));
    }

    #[test]
    fn test_reader_role() {
        let reader = Role::reader();
        assert!(reader.has_permission(Permission::Read));
        assert!(!reader.has_permission(Permission::Write));
        assert!(!reader.has_permission(Permission::Admin));
    }

    #[test]
    fn test_writer_role() {
        let writer = Role::writer();
        assert!(writer.has_permission(Permission::Read));
        assert!(writer.has_permission(Permission::Write));
        assert!(!writer.has_permission(Permission::Admin));
    }

    #[test]
    fn test_builtin_roles() {
        assert!(Role::builtin("admin").is_some());
        assert!(Role::builtin("READER").is_some());
        assert!(Role::builtin("Writer").is_some());
        assert!(Role::builtin("unknown").is_none());
    }
}
