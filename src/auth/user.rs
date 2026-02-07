//! User definitions for RBAC

use super::{Permission, Role};
use super::role_store::RoleStore;
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A user with authentication credentials and authorization info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Username (unique identifier)
    pub username: String,

    /// Password hash (Argon2)
    #[serde(skip_serializing)]
    pub password_hash: String,

    /// Role names assigned to this user
    pub roles: Vec<String>,

    /// Optional: restrict access to specific databases
    /// If empty/None, user has access to all databases (subject to permissions)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub databases: Option<Vec<String>>,

    /// Optional: restrict access to specific tables (format: "database.schema.table")
    /// If empty/None, user has access to all tables in allowed databases
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
}

impl User {
    /// Create a new user
    pub fn new(
        username: &str,
        password_hash: &str,
        roles: Vec<String>,
        databases: Option<Vec<String>>,
    ) -> Self {
        Self {
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            roles,
            databases,
            tables: None,
        }
    }

    /// Verify a password against this user's hash
    pub fn verify_password(&self, password: &str) -> bool {
        let parsed_hash = match PasswordHash::new(&self.password_hash) {
            Ok(h) => h,
            Err(_) => return false,
        };

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    /// Get effective permissions from all assigned roles (built-in only)
    pub fn permissions(&self) -> HashSet<Permission> {
        let mut perms = HashSet::new();
        for role_name in &self.roles {
            if let Some(role) = Role::builtin(role_name) {
                perms.extend(role.permissions);
            }
        }
        perms
    }

    /// Get effective permissions using a role store (supports custom roles)
    pub fn permissions_with_store(&self, role_store: &RoleStore) -> HashSet<Permission> {
        let mut perms = HashSet::new();
        for role_name in &self.roles {
            if let Some(role) = role_store.get_role(role_name) {
                perms.extend(role.permissions);
            }
        }
        perms
    }

    /// Check if user has a specific permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.permissions().contains(&permission)
    }

    /// Check if user has access to a specific database
    pub fn can_access_database(&self, database: &str) -> bool {
        match &self.databases {
            Some(allowed) => {
                allowed.iter().any(|db| {
                    // Support wildcard patterns like "prod_*"
                    if db.ends_with('*') {
                        let prefix = &db[..db.len() - 1];
                        database.starts_with(prefix)
                    } else {
                        db == database
                    }
                })
            }
            // No restriction = access to all databases
            None => true,
        }
    }

    /// Check if user has access to a specific table
    pub fn can_access_table(&self, database: &str, schema: &str, table: &str) -> bool {
        // First check database access
        if !self.can_access_database(database) {
            return false;
        }

        // Then check table restrictions if any
        match &self.tables {
            Some(allowed) => {
                let full_name = format!("{}.{}.{}", database, schema, table);
                let db_schema = format!("{}.{}.*", database, schema);
                let db_all = format!("{}.*", database);

                allowed.iter().any(|t| {
                    t == &full_name || t == &db_schema || t == &db_all || t == "*"
                })
            }
            // No table restriction = access to all tables in allowed databases
            None => true,
        }
    }

    /// Check if user is an admin (has Admin permission)
    pub fn is_admin(&self) -> bool {
        self.has_permission(Permission::Admin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_password;

    #[test]
    fn test_user_permissions() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["writer".to_string()],
            None,
        );

        assert!(user.has_permission(Permission::Read));
        assert!(user.has_permission(Permission::Write));
        assert!(!user.has_permission(Permission::Admin));
    }

    #[test]
    fn test_database_access() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec!["prod".to_string(), "test_*".to_string()]),
        );

        assert!(user.can_access_database("prod"));
        assert!(user.can_access_database("test_db"));
        assert!(user.can_access_database("test_analytics"));
        assert!(!user.can_access_database("staging"));
    }

    #[test]
    fn test_table_access() {
        let mut user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec!["mydb".to_string()]),
        );
        user.tables = Some(vec!["mydb.default.cpu".to_string()]);

        assert!(user.can_access_table("mydb", "default", "cpu"));
        assert!(!user.can_access_table("mydb", "default", "memory"));
        assert!(!user.can_access_table("other", "default", "cpu"));
    }

    #[test]
    fn test_unrestricted_user() {
        let user = User::new(
            "admin",
            &hash_password("admin").unwrap(),
            vec!["admin".to_string()],
            None,
        );

        assert!(user.can_access_database("any_database"));
        assert!(user.can_access_table("any", "schema", "table"));
        assert!(user.is_admin());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    #[test]
    fn test_empty_password() {
        let user = User::new(
            "test",
            &hash_password("").unwrap(),
            vec!["reader".to_string()],
            None,
        );
        // Empty password should still work if hashed correctly
        assert!(user.verify_password(""));
        assert!(!user.verify_password("not_empty"));
    }

    #[test]
    fn test_special_characters_in_password() {
        let special_passwords: Vec<String> = vec![
            "pass:word".to_string(),           // colon (used in Basic Auth)
            "pass word".to_string(),           // space
            "pässwörd".to_string(),            // unicode
            "p@ss!w#rd$%^&*()".to_string(),    // special chars
            "pass\nword".to_string(),          // newline
            "pass\tword".to_string(),          // tab
            "密码测试".to_string(),            // Chinese characters
            "🔐🔑".to_string(),                // emoji
            "a".repeat(1000),                  // very long password
        ];

        for pwd in special_passwords {
            let hash = hash_password(&pwd).unwrap();
            let user = User::new("test", &hash, vec!["reader".to_string()], None);
            assert!(
                user.verify_password(&pwd),
                "Failed for password: {:?}",
                pwd
            );
            assert!(!user.verify_password("wrong"));
        }
    }

    #[test]
    fn test_invalid_password_hash() {
        let user = User::new(
            "test",
            "not_a_valid_hash",
            vec!["reader".to_string()],
            None,
        );
        // Should return false, not panic
        assert!(!user.verify_password("anything"));
    }

    #[test]
    fn test_empty_roles() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec![], // No roles
            None,
        );
        // User with no roles has no permissions
        assert!(!user.has_permission(Permission::Read));
        assert!(!user.has_permission(Permission::Write));
        assert!(!user.has_permission(Permission::Admin));
        assert!(!user.is_admin());
    }

    #[test]
    fn test_unknown_role() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["nonexistent_role".to_string()],
            None,
        );
        // Unknown roles give no permissions
        assert!(!user.has_permission(Permission::Read));
        assert!(!user.has_permission(Permission::Write));
        assert!(!user.has_permission(Permission::Admin));
    }

    #[test]
    fn test_multiple_roles() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string(), "writer".to_string()],
            None,
        );
        // Should have combined permissions
        assert!(user.has_permission(Permission::Read));
        assert!(user.has_permission(Permission::Write));
        assert!(!user.has_permission(Permission::Admin));
    }

    #[test]
    fn test_duplicate_roles() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec![
                "reader".to_string(),
                "reader".to_string(),
                "reader".to_string(),
            ],
            None,
        );
        // Duplicate roles should work fine
        assert!(user.has_permission(Permission::Read));
        assert!(!user.has_permission(Permission::Write));
    }

    #[test]
    fn test_empty_databases_list() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec![]), // Empty list (different from None)
        );
        // Empty list = no database access
        assert!(!user.can_access_database("any"));
        assert!(!user.can_access_database(""));
    }

    #[test]
    fn test_empty_tables_list() {
        let mut user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec!["mydb".to_string()]),
        );
        user.tables = Some(vec![]); // Empty list

        // Empty table list = no table access
        assert!(!user.can_access_table("mydb", "default", "cpu"));
    }

    #[test]
    fn test_database_wildcard_patterns() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec![
                "prod_*".to_string(),
                "staging".to_string(),
                "*".to_string(), // This would match everything with prefix ""
            ]),
        );

        assert!(user.can_access_database("prod_us"));
        assert!(user.can_access_database("prod_eu"));
        assert!(user.can_access_database("prod_")); // Edge: just the prefix
        assert!(user.can_access_database("staging"));
        assert!(user.can_access_database("anything")); // * matches everything
    }

    #[test]
    fn test_table_wildcard_patterns() {
        let mut user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec!["mydb".to_string(), "other".to_string()]),
        );
        user.tables = Some(vec![
            "mydb.default.*".to_string(),  // All tables in default schema
            "mydb.*".to_string(),          // All schemas in mydb
            "other.public.cpu".to_string(), // Specific table
        ]);

        // mydb.default.* matches
        assert!(user.can_access_table("mydb", "default", "cpu"));
        assert!(user.can_access_table("mydb", "default", "memory"));

        // mydb.* matches all schemas
        assert!(user.can_access_table("mydb", "other_schema", "any_table"));

        // specific table
        assert!(user.can_access_table("other", "public", "cpu"));
        assert!(!user.can_access_table("other", "public", "memory"));
    }

    #[test]
    fn test_global_table_wildcard() {
        let mut user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            None, // All databases
        );
        user.tables = Some(vec!["*".to_string()]);

        // * should match everything
        assert!(user.can_access_table("any", "schema", "table"));
        assert!(user.can_access_table("x", "y", "z"));
    }

    #[test]
    fn test_table_without_database_access() {
        let mut user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec!["allowed_db".to_string()]),
        );
        user.tables = Some(vec!["other_db.default.cpu".to_string()]);

        // Even with table permission, database check fails first
        assert!(!user.can_access_table("other_db", "default", "cpu"));
    }

    #[test]
    fn test_special_characters_in_database_names() {
        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            Some(vec![
                "my-db".to_string(),
                "my.db".to_string(),
                "my_db".to_string(),
                "123db".to_string(),
            ]),
        );

        assert!(user.can_access_database("my-db"));
        assert!(user.can_access_database("my.db"));
        assert!(user.can_access_database("my_db"));
        assert!(user.can_access_database("123db"));
        assert!(!user.can_access_database("mydb"));
    }

    #[test]
    fn test_case_sensitivity() {
        let user = User::new(
            "TestUser",
            &hash_password("TestPass").unwrap(),
            vec!["reader".to_string()],
            Some(vec!["MyDB".to_string()]),
        );

        // Passwords are case-sensitive
        assert!(user.verify_password("TestPass"));
        assert!(!user.verify_password("testpass"));
        assert!(!user.verify_password("TESTPASS"));

        // Database names are case-sensitive
        assert!(user.can_access_database("MyDB"));
        assert!(!user.can_access_database("mydb"));
        assert!(!user.can_access_database("MYDB"));
    }

    #[test]
    fn test_username_with_special_chars() {
        let special_usernames = vec![
            "user@domain.com",
            "user:name",
            "user name",
            "用户",
            "user\nname",
        ];

        for username in special_usernames {
            let user = User::new(
                username,
                &hash_password("test").unwrap(),
                vec!["reader".to_string()],
                None,
            );
            assert_eq!(user.username, username);
        }
    }
}
