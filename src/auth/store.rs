//! User store implementations

use super::User;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use tracing::{debug, info, warn};

/// Trait for user storage backends
pub trait UserStore: Send + Sync {
    /// Get a user by username
    fn get_user(&self, username: &str) -> Option<User>;

    /// Authenticate a user with username and password
    fn authenticate(&self, username: &str, password: &str) -> Option<User> {
        let user = self.get_user(username)?;
        if user.verify_password(password) {
            Some(user)
        } else {
            None
        }
    }

    /// List all usernames
    fn list_users(&self) -> Vec<String>;

    /// Check if any users exist
    fn has_users(&self) -> bool {
        !self.list_users().is_empty()
    }

    /// Add or update a user (for runtime management)
    fn upsert_user(&self, _user: User) -> Result<(), String> {
        Err("User management not supported by this store".to_string())
    }

    /// Remove a user by username
    fn remove_user(&self, _username: &str) -> Result<bool, String> {
        Err("User management not supported by this store".to_string())
    }
}

/// File-based user store (users defined in config)
pub struct FileUserStore {
    users: RwLock<HashMap<String, User>>,
}

impl FileUserStore {
    /// Create a new file-based user store from user configurations
    pub fn new(users: Vec<UserConfig>) -> Self {
        let mut map = HashMap::new();
        for config in users {
            let user = User {
                username: config.username.clone(),
                password_hash: config.password_hash,
                roles: config.roles,
                databases: config.databases,
                tables: config.tables,
            };
            map.insert(config.username, user);
        }
        info!("Loaded {} users from configuration", map.len());
        Self {
            users: RwLock::new(map),
        }
    }

    /// Create an empty store
    pub fn empty() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
        }
    }

    /// Load users from a TOML file
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read users file: {}", e))?;

        let config: UsersFileConfig = toml::from_str(&content)
            .map_err(|e| format!("Failed to parse users file: {}", e))?;

        Ok(Self::new(config.users))
    }

    /// Add or update a user (for runtime management)
    pub fn upsert_user(&self, user: User) {
        let mut users = self.users.write().unwrap();
        debug!("Upserting user: {}", user.username);
        users.insert(user.username.clone(), user);
    }

    /// Remove a user
    pub fn remove_user(&self, username: &str) -> bool {
        let mut users = self.users.write().unwrap();
        users.remove(username).is_some()
    }
}

impl UserStore for FileUserStore {
    fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read().unwrap();
        users.get(username).cloned()
    }

    fn list_users(&self) -> Vec<String> {
        let users = self.users.read().unwrap();
        users.keys().cloned().collect()
    }

    fn upsert_user(&self, user: User) -> Result<(), String> {
        self.upsert_user(user);
        Ok(())
    }

    fn remove_user(&self, username: &str) -> Result<bool, String> {
        Ok(self.remove_user(username))
    }
}

/// User configuration from TOML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserConfig {
    /// Username
    pub username: String,

    /// Password hash (Argon2)
    pub password_hash: String,

    /// Role names assigned to this user
    #[serde(default)]
    pub roles: Vec<String>,

    /// Optional: restrict access to specific databases
    #[serde(default)]
    pub databases: Option<Vec<String>>,

    /// Optional: restrict access to specific tables
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

/// Config file structure for users.toml
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UsersFileConfig {
    #[serde(default)]
    users: Vec<UserConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_password;

    #[test]
    fn test_file_user_store() {
        let users = vec![
            UserConfig {
                username: "admin".to_string(),
                password_hash: hash_password("admin123").unwrap(),
                roles: vec!["admin".to_string()],
                databases: None,
                tables: None,
            },
            UserConfig {
                username: "reader".to_string(),
                password_hash: hash_password("reader123").unwrap(),
                roles: vec!["reader".to_string()],
                databases: Some(vec!["analytics".to_string()]),
                tables: None,
            },
        ];

        let store = FileUserStore::new(users);

        // Test get_user
        assert!(store.get_user("admin").is_some());
        assert!(store.get_user("reader").is_some());
        assert!(store.get_user("unknown").is_none());

        // Test authenticate
        assert!(store.authenticate("admin", "admin123").is_some());
        assert!(store.authenticate("admin", "wrong").is_none());
        assert!(store.authenticate("unknown", "any").is_none());

        // Test list_users
        let usernames = store.list_users();
        assert_eq!(usernames.len(), 2);
        assert!(usernames.contains(&"admin".to_string()));
        assert!(usernames.contains(&"reader".to_string()));
    }

    #[test]
    fn test_upsert_remove() {
        let store = FileUserStore::empty();

        let user = User::new(
            "test",
            &hash_password("test").unwrap(),
            vec!["reader".to_string()],
            None,
        );

        store.upsert_user(user);
        assert!(store.get_user("test").is_some());

        store.remove_user("test");
        assert!(store.get_user("test").is_none());
    }
}
