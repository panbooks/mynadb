//! Authentication and Authorization (RBAC) module
//!
//! This module provides:
//! - Multi-user support with file-based user storage
//! - Password hashing with Argon2
//! - Role-based access control (RBAC)
//! - Database and table-level permissions
//!
//! # Permission Model
//!
//! - `Read`: SELECT queries, schema inspection
//! - `Write`: INSERT data via write endpoints
//! - `Admin`: DELETE partitions, compaction, retention, cache management
//!
//! # Configuration
//!
//! ```toml
//! [auth]
//! enabled = true
//!
//! [[auth.users]]
//! username = "admin"
//! password_hash = "$argon2id$..."
//! roles = ["admin"]
//!
//! [[auth.users]]
//! username = "reader"
//! password_hash = "$argon2id$..."
//! roles = ["reader"]
//! databases = ["analytics"]  # Optional: restrict to specific databases
//! ```

mod context;
mod permission;
mod policy;
mod role;
mod store;
mod user;

pub use context::AuthContext;
pub use permission::Permission;
pub use policy::Policy;
pub use role::Role;
pub use store::{FileUserStore, UserConfig, UserStore};
pub use user::User;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2,
};

/// Hash a password using Argon2id
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| format!("Failed to hash password: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_password() {
        let hash = hash_password("test_password").unwrap();
        assert!(hash.starts_with("$argon2"));
    }

    #[test]
    fn test_verify_hashed_password() {
        let password = "my_secret_password";
        let hash = hash_password(password).unwrap();

        let user = User::new("test", &hash, vec!["admin".to_string()], None);
        assert!(user.verify_password(password));
        assert!(!user.verify_password("wrong_password"));
    }
}
