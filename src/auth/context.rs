//! Authentication context for request handling

use super::{Permission, User};
use std::sync::Arc;

/// Authentication context passed through request handlers
///
/// This provides information about the authenticated user and their permissions
/// to be used throughout the request lifecycle.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The authenticated user (None if auth is disabled)
    user: Option<Arc<User>>,

    /// Whether authentication is enabled
    auth_enabled: bool,
}

impl AuthContext {
    /// Create a new auth context with an authenticated user
    pub fn authenticated(user: User) -> Self {
        Self {
            user: Some(Arc::new(user)),
            auth_enabled: true,
        }
    }

    /// Create an anonymous context (auth disabled)
    pub fn anonymous() -> Self {
        Self {
            user: None,
            auth_enabled: false,
        }
    }

    /// Check if authentication is enabled
    pub fn is_auth_enabled(&self) -> bool {
        self.auth_enabled
    }

    /// Get the authenticated user (if any)
    pub fn user(&self) -> Option<&User> {
        self.user.as_deref()
    }

    /// Get the username (if authenticated)
    pub fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }

    /// Check if user has a specific permission
    ///
    /// Returns true if:
    /// - Auth is disabled (anonymous access)
    /// - User is authenticated and has the permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        if !self.auth_enabled {
            return true;
        }
        self.user
            .as_ref()
            .map(|u| u.has_permission(permission))
            .unwrap_or(false)
    }

    /// Check if user can access a database
    ///
    /// Returns true if:
    /// - Auth is disabled
    /// - User has access to the database
    pub fn can_access_database(&self, database: &str) -> bool {
        if !self.auth_enabled {
            return true;
        }
        self.user
            .as_ref()
            .map(|u| u.can_access_database(database))
            .unwrap_or(false)
    }

    /// Check if user can access a table
    ///
    /// Returns true if:
    /// - Auth is disabled
    /// - User has access to the table
    pub fn can_access_table(&self, database: &str, schema: &str, table: &str) -> bool {
        if !self.auth_enabled {
            return true;
        }
        self.user
            .as_ref()
            .map(|u| u.can_access_table(database, schema, table))
            .unwrap_or(false)
    }

    /// Check if user can perform a read operation on a database
    pub fn can_read(&self, database: &str) -> bool {
        self.has_permission(Permission::Read) && self.can_access_database(database)
    }

    /// Check if user can perform a write operation on a database
    pub fn can_write(&self, database: &str) -> bool {
        self.has_permission(Permission::Write) && self.can_access_database(database)
    }

    /// Check if user can perform admin operations on a database
    pub fn can_admin(&self, database: &str) -> bool {
        self.has_permission(Permission::Admin) && self.can_access_database(database)
    }

    /// Check if user has global admin permission (not restricted to specific databases)
    pub fn is_global_admin(&self) -> bool {
        if !self.auth_enabled {
            return true;
        }
        match &self.user {
            Some(u) => u.has_permission(Permission::Admin) && u.databases.is_none(),
            None => false,
        }
    }
}

impl Default for AuthContext {
    fn default() -> Self {
        Self::anonymous()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_password;

    #[test]
    fn test_anonymous_context() {
        let ctx = AuthContext::anonymous();

        assert!(!ctx.is_auth_enabled());
        assert!(ctx.user().is_none());
        assert!(ctx.has_permission(Permission::Admin));
        assert!(ctx.can_access_database("any"));
        assert!(ctx.can_read("any"));
        assert!(ctx.can_write("any"));
        assert!(ctx.can_admin("any"));
    }

    #[test]
    fn test_authenticated_context() {
        let user = User::new(
            "writer",
            &hash_password("test").unwrap(),
            vec!["writer".to_string()],
            Some(vec!["mydb".to_string()]),
        );
        let ctx = AuthContext::authenticated(user);

        assert!(ctx.is_auth_enabled());
        assert!(ctx.user().is_some());
        assert_eq!(ctx.username(), Some("writer"));

        // Permission checks
        assert!(ctx.has_permission(Permission::Read));
        assert!(ctx.has_permission(Permission::Write));
        assert!(!ctx.has_permission(Permission::Admin));

        // Database access checks
        assert!(ctx.can_access_database("mydb"));
        assert!(!ctx.can_access_database("other"));

        // Combined checks
        assert!(ctx.can_read("mydb"));
        assert!(ctx.can_write("mydb"));
        assert!(!ctx.can_admin("mydb"));
        assert!(!ctx.can_read("other"));
    }

    #[test]
    fn test_global_admin() {
        let admin = User::new(
            "admin",
            &hash_password("test").unwrap(),
            vec!["admin".to_string()],
            None, // No database restriction
        );
        let ctx = AuthContext::authenticated(admin);

        assert!(ctx.is_global_admin());
        assert!(ctx.can_admin("any_database"));
    }

    #[test]
    fn test_restricted_admin() {
        let admin = User::new(
            "db_admin",
            &hash_password("test").unwrap(),
            vec!["admin".to_string()],
            Some(vec!["prod".to_string()]), // Restricted to prod database
        );
        let ctx = AuthContext::authenticated(admin);

        assert!(!ctx.is_global_admin());
        assert!(ctx.can_admin("prod"));
        assert!(!ctx.can_admin("dev"));
    }
}
