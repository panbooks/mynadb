//! Role store for custom role definitions
//!
//! Manages both built-in roles (admin, reader, writer) and custom roles
//! defined in configuration. Custom roles map a role name to a set of
//! permissions (read, write, admin).

use super::{Permission, Role};
use std::collections::HashMap;
use std::sync::RwLock;

/// Configuration for a custom role
#[derive(Debug, Clone)]
pub struct CustomRoleConfig {
    pub name: String,
    pub permissions: Vec<String>,
}

/// Store for role definitions (built-in + custom)
pub struct RoleStore {
    custom_roles: RwLock<HashMap<String, Role>>,
}

impl RoleStore {
    /// Create a new role store from custom role configurations
    pub fn new(custom_roles: Vec<CustomRoleConfig>) -> Self {
        let mut map = HashMap::new();
        for config in custom_roles {
            let permissions: Vec<Permission> = config
                .permissions
                .iter()
                .filter_map(|p| Permission::from_str(p))
                .collect();
            let role = Role::new(&config.name, permissions);
            map.insert(config.name.to_lowercase(), role);
        }
        Self {
            custom_roles: RwLock::new(map),
        }
    }

    /// Create an empty role store (built-in roles only)
    pub fn empty() -> Self {
        Self {
            custom_roles: RwLock::new(HashMap::new()),
        }
    }

    /// Get a role by name, checking built-in roles first, then custom
    pub fn get_role(&self, name: &str) -> Option<Role> {
        // Built-in roles take precedence
        if let Some(role) = Role::builtin(name) {
            return Some(role);
        }
        // Check custom roles
        let roles = self.custom_roles.read().unwrap();
        roles.get(&name.to_lowercase()).cloned()
    }

    /// Add or update a custom role
    pub fn upsert_role(&self, name: &str, permissions: Vec<Permission>) {
        let role = Role::new(name, permissions);
        let mut roles = self.custom_roles.write().unwrap();
        roles.insert(name.to_lowercase(), role);
    }

    /// Remove a custom role (cannot remove built-in roles)
    pub fn remove_role(&self, name: &str) -> bool {
        if Role::builtin(name).is_some() {
            return false; // Cannot remove built-in roles
        }
        let mut roles = self.custom_roles.write().unwrap();
        roles.remove(&name.to_lowercase()).is_some()
    }

    /// List all role names (built-in + custom)
    pub fn list_roles(&self) -> Vec<String> {
        let mut names: Vec<String> = Role::builtin_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let roles = self.custom_roles.read().unwrap();
        for name in roles.keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_role_store() {
        let store = RoleStore::empty();
        // Built-in roles should still work
        assert!(store.get_role("admin").is_some());
        assert!(store.get_role("reader").is_some());
        assert!(store.get_role("writer").is_some());
        assert!(store.get_role("analyst").is_none());
    }

    #[test]
    fn test_custom_roles() {
        let store = RoleStore::new(vec![CustomRoleConfig {
            name: "analyst".to_string(),
            permissions: vec!["read".to_string()],
        }]);

        let role = store.get_role("analyst").unwrap();
        assert!(role.has_permission(Permission::Read));
        assert!(!role.has_permission(Permission::Write));
        assert!(!role.has_permission(Permission::Admin));
    }

    #[test]
    fn test_builtin_precedence() {
        // Even if someone defines a custom "admin" role with fewer perms,
        // the built-in admin should take precedence
        let store = RoleStore::new(vec![CustomRoleConfig {
            name: "admin".to_string(),
            permissions: vec!["read".to_string()],
        }]);

        let role = store.get_role("admin").unwrap();
        assert!(role.has_permission(Permission::Admin));
    }

    #[test]
    fn test_case_insensitive() {
        let store = RoleStore::new(vec![CustomRoleConfig {
            name: "Analyst".to_string(),
            permissions: vec!["read".to_string()],
        }]);

        assert!(store.get_role("analyst").is_some());
        assert!(store.get_role("ANALYST").is_some());
    }

    #[test]
    fn test_upsert_remove() {
        let store = RoleStore::empty();

        store.upsert_role("analyst", vec![Permission::Read]);
        assert!(store.get_role("analyst").is_some());

        assert!(store.remove_role("analyst"));
        assert!(store.get_role("analyst").is_none());
    }

    #[test]
    fn test_cannot_remove_builtin() {
        let store = RoleStore::empty();
        assert!(!store.remove_role("admin"));
        assert!(!store.remove_role("reader"));
        assert!(!store.remove_role("writer"));
    }

    #[test]
    fn test_list_roles() {
        let store = RoleStore::new(vec![CustomRoleConfig {
            name: "analyst".to_string(),
            permissions: vec!["read".to_string()],
        }]);

        let roles = store.list_roles();
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"reader".to_string()));
        assert!(roles.contains(&"writer".to_string()));
        assert!(roles.contains(&"analyst".to_string()));
    }

    #[test]
    fn test_invalid_permissions_ignored() {
        let store = RoleStore::new(vec![CustomRoleConfig {
            name: "broken".to_string(),
            permissions: vec!["read".to_string(), "invalid".to_string()],
        }]);

        let role = store.get_role("broken").unwrap();
        assert!(role.has_permission(Permission::Read));
        assert!(!role.has_permission(Permission::Write));
    }
}
