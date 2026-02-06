//! Authentication and authorization middleware for HTTP API
//!
//! This module provides RBAC-based authentication and authorization:
//! - Basic auth with password hash verification
//! - Query parameter auth (u=username&p=password)
//! - Role-based permission checking
//! - Database and table-level access control

use crate::auth::{AuthContext, FileUserStore, Permission, Policy, UserStore};
use crate::config::AuthConfig;
use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose, Engine as _};
use std::sync::Arc;
use tracing::{debug, warn};

/// Authentication middleware state
pub struct AuthMiddleware {
    /// User store for authentication
    user_store: Arc<dyn UserStore>,
    /// Whether auth is enabled
    enabled: bool,
}

impl AuthMiddleware {
    /// Create new auth middleware from config
    pub fn new(config: &AuthConfig) -> Self {
        let user_store: Arc<dyn UserStore> = if config.enabled {
            // Load users from config or file
            let users = if let Some(ref path) = config.users_file {
                match FileUserStore::from_file(std::path::Path::new(path)) {
                    Ok(store) => return Self {
                        user_store: Arc::new(store),
                        enabled: true,
                    },
                    Err(e) => {
                        warn!("Failed to load users file '{}': {}, falling back to config", path, e);
                        config.users.clone()
                    }
                }
            } else {
                config.users.clone()
            };

            // Convert config users to store
            let store_users: Vec<crate::auth::UserConfig> = users
                .iter()
                .map(|u| crate::auth::UserConfig {
                    username: u.username.clone(),
                    password_hash: u.password_hash.clone(),
                    roles: u.roles.clone(),
                    databases: u.databases.clone(),
                    tables: u.tables.clone(),
                })
                .collect();

            Arc::new(FileUserStore::new(store_users))
        } else {
            Arc::new(FileUserStore::empty())
        };

        Self {
            user_store,
            enabled: config.enabled,
        }
    }

    /// Check if auth is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get user store for other services (MySQL, Flight)
    pub fn user_store(&self) -> Arc<dyn UserStore> {
        self.user_store.clone()
    }

    /// Extract credentials from query parameters (u and p)
    fn extract_query_credentials(query: Option<&str>) -> Option<(String, String)> {
        let query_str = query?;
        let mut username: Option<String> = None;
        let mut password: Option<String> = None;

        for param in query_str.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "u" => username = urlencoding::decode(value).ok().map(|s| s.into_owned()),
                    "p" => password = urlencoding::decode(value).ok().map(|s| s.into_owned()),
                    _ => {}
                }
            }
        }

        match (username, password) {
            (Some(u), Some(p)) => Some((u, p)),
            _ => None,
        }
    }

    /// Extract credentials from Basic Auth header
    fn extract_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
        let auth_header = headers.get("authorization")?;
        let auth_str = auth_header.to_str().ok()?;

        if !auth_str.starts_with("Basic ") {
            return None;
        }

        let encoded = &auth_str[6..];
        let decoded = general_purpose::STANDARD.decode(encoded).ok()?;
        let credentials = String::from_utf8(decoded).ok()?;

        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }

        Some((parts[0].to_string(), parts[1].to_string()))
    }

    /// Authenticate request and return auth context
    pub fn authenticate(&self, headers: &HeaderMap, query: Option<&str>) -> AuthContext {
        if !self.enabled {
            return AuthContext::anonymous();
        }

        // Try query parameter auth first
        if let Some((username, password)) = Self::extract_query_credentials(query) {
            if let Some(user) = self.user_store.authenticate(&username, &password) {
                debug!("Authenticated user via query params: {}", username);
                return AuthContext::authenticated(user);
            }
        }

        // Fall back to Basic Auth
        if let Some((username, password)) = Self::extract_basic_auth(headers) {
            if let Some(user) = self.user_store.authenticate(&username, &password) {
                debug!("Authenticated user via Basic Auth: {}", username);
                return AuthContext::authenticated(user);
            }
        }

        // No valid credentials
        AuthContext::anonymous()
    }

    /// Check authorization for a request
    pub fn authorize(&self, ctx: &AuthContext, method: &str, path: &str) -> Result<(), AuthError> {
        // If auth disabled, allow everything
        if !self.enabled {
            return Ok(());
        }

        // Check if endpoint is public
        if Policy::is_public_endpoint(path) {
            return Ok(());
        }

        // Must be authenticated for non-public endpoints
        if ctx.user().is_none() {
            return Err(AuthError::Unauthenticated);
        }

        // Get required permission for this endpoint
        let (required_permission, requires_db_access) = match Policy::required_permission(method, path) {
            Some(p) => p,
            None => return Ok(()), // Public endpoint
        };

        // Check permission
        if !ctx.has_permission(required_permission) {
            return Err(AuthError::Forbidden(format!(
                "Missing permission: {}",
                required_permission
            )));
        }

        // Check database access if required
        if requires_db_access {
            if let Some(database) = Policy::extract_database(path) {
                if !ctx.can_access_database(&database) {
                    return Err(AuthError::Forbidden(format!(
                        "Access denied to database: {}",
                        database
                    )));
                }
            }
        }

        // Check table access if applicable
        if let Some((db, schema, table)) = Policy::extract_table(path) {
            if !ctx.can_access_table(&db, &schema, &table) {
                return Err(AuthError::Forbidden(format!(
                    "Access denied to table: {}.{}.{}",
                    db, schema, table
                )));
            }
        }

        Ok(())
    }
}

/// Authentication/authorization error
#[derive(Debug)]
pub enum AuthError {
    /// Not authenticated
    Unauthenticated,
    /// Authenticated but not authorized
    Forbidden(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        match self {
            AuthError::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                [("WWW-Authenticate", "Basic realm=\"MynaDB\"")],
                "Authentication required",
            )
                .into_response(),
            AuthError::Forbidden(msg) => {
                (StatusCode::FORBIDDEN, format!("Forbidden: {}", msg)).into_response()
            }
        }
    }
}

/// Middleware function for authentication and authorization
pub async fn auth_middleware_fn(
    axum::Extension(auth): axum::Extension<Arc<AuthMiddleware>>,
    mut req: Request,
    next: Next,
) -> Response {
    let method = req.method().as_str().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|s| s.to_string());

    // Authenticate
    let ctx = auth.authenticate(req.headers(), query.as_deref());

    // Authorize
    if let Err(e) = auth.authorize(&ctx, &method, &path) {
        warn!(
            "Authorization failed for {} {}: {:?} (user: {:?})",
            method,
            path,
            e,
            ctx.username()
        );
        return e.into_response();
    }

    // Store auth context in request extensions for handlers
    req.extensions_mut().insert(ctx);

    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_password;
    use crate::config::AuthUserConfig;

    fn test_config_with_users() -> AuthConfig {
        AuthConfig {
            enabled: true,
            users: vec![
                AuthUserConfig {
                    username: "admin".to_string(),
                    password_hash: hash_password("admin123").unwrap(),
                    roles: vec!["admin".to_string()],
                    databases: None,
                    tables: None,
                },
                AuthUserConfig {
                    username: "reader".to_string(),
                    password_hash: hash_password("reader123").unwrap(),
                    roles: vec!["reader".to_string()],
                    databases: Some(vec!["analytics".to_string()]),
                    tables: None,
                },
            ],
            users_file: None,
        }
    }

    #[test]
    fn test_auth_disabled() {
        let config = AuthConfig::default();
        let middleware = AuthMiddleware::new(&config);

        assert!(!middleware.is_enabled());

        let ctx = middleware.authenticate(&HeaderMap::new(), None);
        assert!(!ctx.is_auth_enabled());
        assert!(ctx.has_permission(Permission::Admin));
    }

    #[test]
    fn test_basic_auth() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Valid credentials
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode("admin:admin123");
        headers.insert("authorization", format!("Basic {}", encoded).parse().unwrap());

        let ctx = middleware.authenticate(&headers, None);
        assert!(ctx.is_auth_enabled());
        assert_eq!(ctx.username(), Some("admin"));
        assert!(ctx.has_permission(Permission::Admin));
    }

    #[test]
    fn test_query_param_auth() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=reader&p=reader123"));
        assert!(ctx.is_auth_enabled());
        assert_eq!(ctx.username(), Some("reader"));
        assert!(ctx.has_permission(Permission::Read));
        assert!(!ctx.has_permission(Permission::Write));
    }

    #[test]
    fn test_authorization_public_endpoint() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);
        let ctx = AuthContext::anonymous();

        // Public endpoints should pass even without auth
        assert!(middleware.authorize(&ctx, "GET", "/health").is_ok());
        assert!(middleware.authorize(&ctx, "GET", "/metrics").is_ok());
    }

    #[test]
    fn test_authorization_permission_check() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Authenticate as reader
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=reader&p=reader123"));

        // Reader can query
        assert!(middleware.authorize(&ctx, "GET", "/query/analytics").is_ok());

        // Reader cannot write
        assert!(middleware.authorize(&ctx, "POST", "/write/analytics").is_err());

        // Reader cannot access other databases
        assert!(middleware.authorize(&ctx, "GET", "/query/production").is_err());
    }

    #[test]
    fn test_authorization_admin_operations() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Authenticate as admin
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode("admin:admin123");
        headers.insert("authorization", format!("Basic {}", encoded).parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);

        // Admin can do everything
        assert!(middleware.authorize(&ctx, "POST", "/compact/mydb").is_ok());
        assert!(middleware.authorize(&ctx, "DELETE", "/partitions/mydb/default/cpu/2024-01-01").is_ok());
        assert!(middleware.authorize(&ctx, "POST", "/flush").is_ok());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    #[test]
    fn test_malformed_basic_auth_header() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Missing "Basic " prefix
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode("admin:admin123");
        headers.insert("authorization", encoded.parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);
        assert!(ctx.user().is_none());

        // Invalid base64
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic not_valid_base64!!!".parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);
        assert!(ctx.user().is_none());

        // Valid base64 but missing colon
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode("adminadmin123");
        headers.insert("authorization", format!("Basic {}", encoded).parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);
        assert!(ctx.user().is_none());

        // Empty credentials
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode(":");
        headers.insert("authorization", format!("Basic {}", encoded).parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);
        assert!(ctx.user().is_none());

        // Bearer token instead of Basic (should be ignored)
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer some_token".parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);
        assert!(ctx.user().is_none());
    }

    #[test]
    fn test_password_with_colon() {
        // Password containing a colon (used in Basic Auth delimiter)
        let config = AuthConfig {
            enabled: true,
            users: vec![AuthUserConfig {
                username: "user".to_string(),
                password_hash: hash_password("pass:word:with:colons").unwrap(),
                roles: vec!["reader".to_string()],
                databases: None,
                tables: None,
            }],
            users_file: None,
        };
        let middleware = AuthMiddleware::new(&config);

        // Basic auth uses splitn(2, ':') so password can contain colons
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode("user:pass:word:with:colons");
        headers.insert("authorization", format!("Basic {}", encoded).parse().unwrap());
        let ctx = middleware.authenticate(&headers, None);
        assert_eq!(ctx.username(), Some("user"));
    }

    #[test]
    fn test_url_encoded_query_params() {
        let config = AuthConfig {
            enabled: true,
            users: vec![AuthUserConfig {
                username: "user@domain.com".to_string(),
                password_hash: hash_password("p@ss&word=test").unwrap(),
                roles: vec!["reader".to_string()],
                databases: None,
                tables: None,
            }],
            users_file: None,
        };
        let middleware = AuthMiddleware::new(&config);

        // URL-encoded special characters
        let query = "u=user%40domain.com&p=p%40ss%26word%3Dtest";
        let ctx = middleware.authenticate(&HeaderMap::new(), Some(query));
        assert_eq!(ctx.username(), Some("user@domain.com"));
    }

    #[test]
    fn test_query_params_incomplete() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Only username
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=admin"));
        assert!(ctx.user().is_none());

        // Only password
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("p=admin123"));
        assert!(ctx.user().is_none());

        // Empty values
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=&p="));
        assert!(ctx.user().is_none());

        // Malformed query string
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=admin&p"));
        assert!(ctx.user().is_none());
    }

    #[test]
    fn test_query_params_take_precedence() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Both Basic Auth and query params present
        let mut headers = HeaderMap::new();
        let encoded = general_purpose::STANDARD.encode("admin:admin123");
        headers.insert("authorization", format!("Basic {}", encoded).parse().unwrap());

        // Query params should take precedence (checked first)
        let ctx = middleware.authenticate(&headers, Some("u=reader&p=reader123"));
        assert_eq!(ctx.username(), Some("reader"));
    }

    #[test]
    fn test_wrong_credentials() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        // Wrong password
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=admin&p=wrongpassword"));
        assert!(ctx.user().is_none());

        // Unknown username
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=unknown&p=password"));
        assert!(ctx.user().is_none());

        // Correct username, wrong case password
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=admin&p=ADMIN123"));
        assert!(ctx.user().is_none());
    }

    #[test]
    fn test_authorization_unauthenticated() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        let ctx = AuthContext::anonymous();

        // Unauthenticated users should fail on protected endpoints
        let result = middleware.authorize(&ctx, "GET", "/query/testdb");
        assert!(matches!(result, Err(AuthError::Unauthenticated)));

        let result = middleware.authorize(&ctx, "POST", "/write/testdb");
        assert!(matches!(result, Err(AuthError::Unauthenticated)));
    }

    #[test]
    fn test_authorization_path_extraction() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=reader&p=reader123"));

        // Path with extra slashes or query params should work
        assert!(middleware.authorize(&ctx, "GET", "/query/analytics").is_ok());

        // Different databases
        assert!(middleware.authorize(&ctx, "GET", "/query/analytics").is_ok());
        assert!(middleware.authorize(&ctx, "GET", "/query/other").is_err());
    }

    #[test]
    fn test_authorization_methods() {
        let config = test_config_with_users();
        let middleware = AuthMiddleware::new(&config);

        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=reader&p=reader123"));

        // GET query allowed
        assert!(middleware.authorize(&ctx, "GET", "/query/analytics").is_ok());

        // POST query also allowed (some clients use POST for large queries)
        assert!(middleware.authorize(&ctx, "POST", "/query/analytics").is_ok());

        // HEAD, OPTIONS should work on public endpoints
        assert!(middleware.authorize(&ctx, "HEAD", "/health").is_ok());
        assert!(middleware.authorize(&ctx, "OPTIONS", "/health").is_ok());
    }

    #[test]
    fn test_empty_username() {
        let config = AuthConfig {
            enabled: true,
            users: vec![AuthUserConfig {
                username: "".to_string(), // Empty username
                password_hash: hash_password("test").unwrap(),
                roles: vec!["reader".to_string()],
                databases: None,
                tables: None,
            }],
            users_file: None,
        };
        let middleware = AuthMiddleware::new(&config);

        // Empty username should still work technically
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=&p=test"));
        assert_eq!(ctx.username(), Some(""));
    }

    #[test]
    fn test_no_users_configured() {
        let config = AuthConfig {
            enabled: true,
            users: vec![],
            users_file: None,
        };
        let middleware = AuthMiddleware::new(&config);

        // With no users, nobody can authenticate
        let ctx = middleware.authenticate(&HeaderMap::new(), Some("u=admin&p=admin"));
        assert!(ctx.user().is_none());

        // But public endpoints should still work
        let anon = AuthContext::anonymous();
        assert!(middleware.authorize(&anon, "GET", "/health").is_ok());
    }

    #[test]
    fn test_auth_error_responses() {
        // Test that AuthError produces correct responses
        let unauth = AuthError::Unauthenticated;
        let response = unauth.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let forbidden = AuthError::Forbidden("test".to_string());
        let response = forbidden.into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_concurrent_auth() {
        use std::thread;

        let config = test_config_with_users();
        let middleware = std::sync::Arc::new(AuthMiddleware::new(&config));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let mw = middleware.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let query = if i % 2 == 0 {
                            "u=admin&p=admin123"
                        } else {
                            "u=reader&p=reader123"
                        };
                        let ctx = mw.authenticate(&HeaderMap::new(), Some(query));
                        assert!(ctx.user().is_some());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
