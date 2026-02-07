//! Audit logging for security events
//!
//! Provides structured JSON audit logs for authentication, authorization,
//! and user management events. Logs are written to a dedicated audit log
//! file separate from application logs.

use crate::config::AuditConfig;
use serde::Serialize;
use std::sync::Arc;
use tracing::info;
use tracing_appender::rolling;
use tracing_subscriber::fmt::MakeWriter;

/// Types of audit events
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    AuthSuccess,
    AuthFailure,
    AccessDenied,
    UserCreated,
    UserUpdated,
    UserDeleted,
    QueryDenied,
}

/// Result of an audited operation
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    Success,
    Failure,
    Denied,
}

/// A structured audit event
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub timestamp: String,
    pub event_type: AuditEventType,
    pub username: String,
    pub source_ip: String,
    pub resource: String,
    pub action: String,
    pub result: AuditResult,
    pub detail: String,
}

/// Audit logger that writes JSON-line formatted events to a rolling file
pub struct AuditLogger {
    enabled: bool,
    writer: Option<Arc<rolling::RollingFileAppender>>,
}

impl AuditLogger {
    /// Create a new audit logger from configuration
    pub fn new(config: &AuditConfig) -> Self {
        if !config.enabled {
            return Self {
                enabled: false,
                writer: None,
            };
        }

        // Create log directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&config.log_dir) {
            tracing::warn!("Failed to create audit log directory '{}': {}", config.log_dir, e);
            return Self {
                enabled: false,
                writer: None,
            };
        }

        let rotation = match config.rotation.as_str() {
            "hourly" => rolling::Rotation::HOURLY,
            "never" => rolling::Rotation::NEVER,
            _ => rolling::Rotation::DAILY,
        };

        let appender = rolling::RollingFileAppender::builder()
            .rotation(rotation)
            .filename_prefix(&config.file_prefix)
            .filename_suffix("log")
            .max_log_files(config.max_files)
            .build(&config.log_dir)
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to create audit log appender: {}", e);
                // Fallback to a basic appender
                rolling::RollingFileAppender::new(
                    rolling::Rotation::DAILY,
                    &config.log_dir,
                    &config.file_prefix,
                )
            });

        info!(
            "Audit logging enabled: dir={}, prefix={}, rotation={}",
            config.log_dir, config.file_prefix, config.rotation
        );

        Self {
            enabled: true,
            writer: Some(Arc::new(appender)),
        }
    }

    /// Create a disabled audit logger
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            writer: None,
        }
    }

    /// Check if audit logging is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Log a raw audit event
    pub fn log_event(&self, event: AuditEvent) {
        if !self.enabled {
            return;
        }

        if let Some(ref writer) = self.writer {
            if let Ok(json) = serde_json::to_string(&event) {
                use std::io::Write;
                let mut w = writer.make_writer();
                let _ = writeln!(w, "{}", json);
            }
        }
    }

    /// Log a successful authentication
    pub fn log_auth_success(&self, username: &str, source_ip: &str, method: &str) {
        self.log_event(AuditEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: AuditEventType::AuthSuccess,
            username: username.to_string(),
            source_ip: source_ip.to_string(),
            resource: String::new(),
            action: "authenticate".to_string(),
            result: AuditResult::Success,
            detail: format!("via {}", method),
        });
    }

    /// Log a failed authentication attempt
    pub fn log_auth_failure(&self, username: &str, source_ip: &str, method: &str) {
        self.log_event(AuditEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: AuditEventType::AuthFailure,
            username: username.to_string(),
            source_ip: source_ip.to_string(),
            resource: String::new(),
            action: "authenticate".to_string(),
            result: AuditResult::Failure,
            detail: format!("via {}", method),
        });
    }

    /// Log an access denied event
    pub fn log_access_denied(&self, username: &str, source_ip: &str, resource: &str, reason: &str) {
        self.log_event(AuditEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: AuditEventType::AccessDenied,
            username: username.to_string(),
            source_ip: source_ip.to_string(),
            resource: resource.to_string(),
            action: "access".to_string(),
            result: AuditResult::Denied,
            detail: reason.to_string(),
        });
    }

    /// Log a user management operation
    pub fn log_user_management(&self, admin_username: &str, action: &str, target_user: &str) {
        self.log_event(AuditEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: match action {
                "user_created" => AuditEventType::UserCreated,
                "user_updated" => AuditEventType::UserUpdated,
                "user_deleted" => AuditEventType::UserDeleted,
                _ => AuditEventType::UserUpdated,
            },
            username: admin_username.to_string(),
            source_ip: String::new(),
            resource: target_user.to_string(),
            action: action.to_string(),
            result: AuditResult::Success,
            detail: format!("{} user: {}", action, target_user),
        });
    }

    /// Log a query access denied event
    pub fn log_query_denied(&self, username: &str, database: &str, detail: &str) {
        self.log_event(AuditEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: AuditEventType::QueryDenied,
            username: username.to_string(),
            source_ip: String::new(),
            resource: database.to_string(),
            action: "query".to_string(),
            result: AuditResult::Denied,
            detail: detail.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_logger() {
        let logger = AuditLogger::disabled();
        assert!(!logger.is_enabled());
        // Should not panic
        logger.log_auth_success("admin", "127.0.0.1", "basic_auth");
        logger.log_auth_failure("admin", "127.0.0.1", "basic_auth");
        logger.log_access_denied("user", "127.0.0.1", "/query/db", "no permission");
        logger.log_user_management("admin", "user_created", "new_user");
        logger.log_query_denied("reader", "restricted_db", "database access denied");
    }

    #[test]
    fn test_audit_event_serialization() {
        let event = AuditEvent {
            timestamp: "2026-02-07T00:00:00Z".to_string(),
            event_type: AuditEventType::AuthSuccess,
            username: "admin".to_string(),
            source_ip: "127.0.0.1".to_string(),
            resource: String::new(),
            action: "authenticate".to_string(),
            result: AuditResult::Success,
            detail: "via basic_auth".to_string(),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("auth_success"));
        assert!(json.contains("admin"));
        assert!(json.contains("127.0.0.1"));
    }

    #[test]
    fn test_default_config_disabled() {
        let config = AuditConfig::default();
        let logger = AuditLogger::new(&config);
        assert!(!logger.is_enabled());
    }
}
