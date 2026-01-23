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

/// Authentication middleware for Axum
pub struct AuthMiddleware {
    config: Arc<AuthConfig>,
}

impl AuthMiddleware {
    pub fn new(config: Arc<AuthConfig>) -> Self {
        AuthMiddleware { config }
    }

    /// Check if the given path is a public endpoint
    fn is_public_endpoint(&self, path: &str) -> bool {
        self.config.public_endpoints.iter().any(|endpoint| {
            if endpoint.ends_with("/*") {
                // Wildcard match: /api/* matches /api/foo
                let prefix = &endpoint[..endpoint.len() - 2];
                path.starts_with(prefix)
            } else {
                // Exact match
                path == endpoint
            }
        })
    }

    /// Validate credentials (username and password)
    fn validate_credentials(&self, username: &str, password: &str) -> bool {
        if username == self.config.username && password == self.config.password {
            debug!("Authentication successful for user: {}", username);
            true
        } else {
            warn!("Authentication failed: invalid credentials");
            false
        }
    }

    /// Extract and validate credentials from query parameters (u and p)
    fn validate_query_auth(&self, query: Option<&str>) -> bool {
        let query_str = match query {
            Some(q) => q,
            None => return false,
        };

        let mut username: Option<String> = None;
        let mut password: Option<String> = None;

        // Parse query parameters manually to extract u and p
        for param in query_str.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "u" => {
                        // URL decode username
                        username = urlencoding::decode(value).ok().map(|s| s.into_owned());
                    }
                    "p" => {
                        // URL decode password
                        password = urlencoding::decode(value).ok().map(|s| s.into_owned());
                    }
                    _ => {}
                }
            }
        }

        match (username, password) {
            (Some(u), Some(p)) => {
                debug!(
                    "Attempting authentication via query parameters (user: {})",
                    u
                );
                self.validate_credentials(&u, &p)
            }
            _ => {
                debug!("Missing u or p query parameter");
                false
            }
        }
    }

    /// Extract and validate Basic Auth credentials from headers
    fn validate_basic_auth(&self, headers: &HeaderMap) -> bool {
        let auth_header = match headers.get("authorization") {
            Some(header) => header,
            None => {
                debug!("Missing Authorization header");
                return false;
            }
        };

        let auth_str = match auth_header.to_str() {
            Ok(s) => s,
            Err(_) => {
                warn!("Invalid Authorization header format");
                return false;
            }
        };

        // Check for "Basic " prefix
        if !auth_str.starts_with("Basic ") {
            warn!("Authorization header missing 'Basic ' prefix");
            return false;
        }

        // Decode Base64 credentials
        let encoded = &auth_str[6..]; // Skip "Basic "
        let decoded = match general_purpose::STANDARD.decode(encoded) {
            Ok(bytes) => bytes,
            Err(_) => {
                warn!("Failed to decode Base64 credentials");
                return false;
            }
        };

        let credentials = match String::from_utf8(decoded) {
            Ok(s) => s,
            Err(_) => {
                warn!("Invalid UTF-8 in decoded credentials");
                return false;
            }
        };

        // Split username:password
        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
        if parts.len() != 2 {
            warn!("Invalid credentials format (expected username:password)");
            return false;
        }

        let (username, password) = (parts[0], parts[1]);

        // Validate credentials
        self.validate_credentials(username, password)
    }

    /// Middleware function to check authentication
    pub async fn check_auth(&self, req: Request, next: Next) -> Response {
        // If auth is disabled, allow all requests
        if !self.config.enabled {
            return next.run(req).await;
        }

        let path = req.uri().path();

        // Check if endpoint is public
        if self.is_public_endpoint(path) {
            debug!("Public endpoint accessed: {}", path);
            return next.run(req).await;
        }

        // Try query parameter authentication first (u and p parameters)
        let query = req.uri().query();
        let auth_valid = if self.validate_query_auth(query) {
            true
        } else {
            // Fall back to Basic Auth header
            self.validate_basic_auth(req.headers())
        };

        if auth_valid {
            next.run(req).await
        } else {
            // Return 401 Unauthorized
            (
                StatusCode::UNAUTHORIZED,
                [("WWW-Authenticate", "Basic realm=\"Stats Service\"")],
                "Authentication required. Please provide valid credentials via Authorization header or u/p query parameters.",
            )
                .into_response()
        }
    }
}

/// Helper function to create the auth middleware layer
pub async fn auth_middleware_fn(
    axum::Extension(auth): axum::Extension<Arc<AuthMiddleware>>,
    req: Request,
    next: Next,
) -> Response {
    auth.check_auth(req, next).await
}
