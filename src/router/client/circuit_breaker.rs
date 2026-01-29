//! Circuit Breaker Pattern Implementation
//!
//! Prevents cascading failures by tracking consecutive failures per node
//! and temporarily blocking requests to unhealthy nodes.
//!
//! ## Configuration
//!
//! The circuit breaker can be configured via `CircuitBreakerConfig`:
//! - `failure_threshold`: Number of consecutive failures before opening (default: 3)
//! - `open_duration_ms`: How long to keep circuit open before testing (default: 10000)
//! - `half_open_max_requests`: Max requests allowed in half-open state (default: 1)
//!
//! ## Example
//!
//! ```ignore
//! let config = CircuitBreakerConfig {
//!     failure_threshold: 5,
//!     open_duration_ms: 30000,
//!     half_open_max_requests: 3,
//! };
//! let cb = CircuitBreaker::with_config(config);
//! ```

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU8, Ordering};
use tracing::{debug, info, warn};

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening circuit
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,

    /// Time to wait (ms) before allowing test requests (half-open state)
    #[serde(default = "default_open_duration_ms")]
    pub open_duration_ms: i64,

    /// Maximum requests allowed in half-open state before deciding
    #[serde(default = "default_half_open_max_requests")]
    pub half_open_max_requests: u32,

    /// Success threshold in half-open to close circuit
    #[serde(default = "default_half_open_success_threshold")]
    pub half_open_success_threshold: u32,
}

fn default_failure_threshold() -> u32 {
    3
}
fn default_open_duration_ms() -> i64 {
    10_000
}
fn default_half_open_max_requests() -> u32 {
    1
}
fn default_half_open_success_threshold() -> u32 {
    1
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            open_duration_ms: default_open_duration_ms(),
            half_open_max_requests: default_half_open_max_requests(),
            half_open_success_threshold: default_half_open_success_threshold(),
        }
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    /// Closed - normal operation, requests flow through
    Closed = 0,
    /// Open - circuit tripped, requests fail fast
    Open = 1,
    /// Half-Open - testing if service recovered
    HalfOpen = 2,
}

/// Circuit breaker for a single Data node
///
/// Tracks consecutive failures and prevents routing to failed nodes.
/// After enough time passes, allows test requests to check recovery.
pub struct CircuitBreaker {
    /// Configuration
    config: CircuitBreakerConfig,
    /// Consecutive failure count
    failures: AtomicU32,
    /// Timestamp of last failure (ms since epoch)
    last_failure_ms: AtomicI64,
    /// Current state: 0=Closed, 1=Open, 2=HalfOpen
    state: AtomicU8,
    /// Half-open request counter
    half_open_requests: AtomicU32,
    /// Half-open success counter
    half_open_successes: AtomicU32,
}

impl CircuitBreaker {
    /// Create with default configuration
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            failures: AtomicU32::new(0),
            last_failure_ms: AtomicI64::new(0),
            state: AtomicU8::new(CircuitState::Closed as u8),
            half_open_requests: AtomicU32::new(0),
            half_open_successes: AtomicU32::new(0),
        }
    }

    /// Record a failure
    pub fn record_failure(&self) {
        let current_state = self.state.load(Ordering::SeqCst);

        if current_state == CircuitState::HalfOpen as u8 {
            // Failure in half-open state - go back to open
            self.state.store(CircuitState::Open as u8, Ordering::SeqCst);
            self.last_failure_ms
                .store(current_timestamp_ms(), Ordering::SeqCst);
            self.half_open_requests.store(0, Ordering::SeqCst);
            self.half_open_successes.store(0, Ordering::SeqCst);
            warn!("Circuit breaker re-opened after failure in half-open state");
            return;
        }

        let failures = self.failures.fetch_add(1, Ordering::SeqCst) + 1;
        self.last_failure_ms
            .store(current_timestamp_ms(), Ordering::SeqCst);

        if failures >= self.config.failure_threshold {
            let old_state = self.state.swap(CircuitState::Open as u8, Ordering::SeqCst);
            if old_state != CircuitState::Open as u8 {
                warn!(
                    "Circuit breaker opened after {} failures (threshold: {})",
                    failures, self.config.failure_threshold
                );
            }
        }
    }

    /// Record a success - may reset or close the circuit breaker
    pub fn record_success(&self) {
        let current_state = self.state.load(Ordering::SeqCst);

        if current_state == CircuitState::HalfOpen as u8 {
            let successes = self.half_open_successes.fetch_add(1, Ordering::SeqCst) + 1;

            if successes >= self.config.half_open_success_threshold {
                // Enough successes in half-open - close the circuit
                self.failures.store(0, Ordering::SeqCst);
                self.half_open_requests.store(0, Ordering::SeqCst);
                self.half_open_successes.store(0, Ordering::SeqCst);
                self.state
                    .store(CircuitState::Closed as u8, Ordering::SeqCst);
                info!(
                    "Circuit breaker closed after {} successful requests in half-open state",
                    successes
                );
            }
        } else {
            // Success in closed state - reset failure counter
            self.failures.store(0, Ordering::SeqCst);
            if current_state != CircuitState::Closed as u8 {
                self.state
                    .store(CircuitState::Closed as u8, Ordering::SeqCst);
                info!("Circuit breaker closed after successful request");
            }
        }
    }

    /// Check if requests are allowed
    ///
    /// Returns true if requests should be allowed, false if circuit is open.
    /// Transitions from Open to HalfOpen after timeout.
    pub fn is_available(&self) -> bool {
        match self.state.load(Ordering::SeqCst) {
            s if s == CircuitState::Closed as u8 => true,
            s if s == CircuitState::Open as u8 => {
                // Check if enough time has passed to try again
                let elapsed = current_timestamp_ms() - self.last_failure_ms.load(Ordering::SeqCst);
                if elapsed > self.config.open_duration_ms {
                    // Transition to half-open, allow test requests
                    self.half_open_requests.store(0, Ordering::SeqCst);
                    self.half_open_successes.store(0, Ordering::SeqCst);
                    self.state
                        .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
                    debug!(
                        "Circuit breaker transitioning to half-open after {}ms",
                        elapsed
                    );
                    true
                } else {
                    false
                }
            }
            s if s == CircuitState::HalfOpen as u8 => {
                // Allow limited requests in half-open state
                let requests = self.half_open_requests.fetch_add(1, Ordering::SeqCst);
                if requests < self.config.half_open_max_requests {
                    true
                } else {
                    // Too many concurrent half-open requests, reject
                    self.half_open_requests.fetch_sub(1, Ordering::SeqCst);
                    false
                }
            }
            _ => false,
        }
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        match self.state.load(Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }

    /// Get failure count
    pub fn failure_count(&self) -> u32 {
        self.failures.load(Ordering::SeqCst)
    }

    /// Get configuration
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.open_duration_ms, 10_000);
        assert_eq!(config.half_open_max_requests, 1);
        assert_eq!(config.half_open_success_threshold, 1);
    }

    #[test]
    fn test_circuit_starts_closed() {
        let cb = CircuitBreaker::new();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_available());
    }

    #[test]
    fn test_circuit_opens_after_threshold() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::with_config(config);

        // First two failures don't open
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        // Third failure opens
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.is_available());
    }

    #[test]
    fn test_success_resets_failures() {
        let cb = CircuitBreaker::new();

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);

        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_custom_config() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            open_duration_ms: 30_000,
            half_open_max_requests: 3,
            half_open_success_threshold: 2,
        };
        let cb = CircuitBreaker::with_config(config);

        // Need 5 failures to open
        for _ in 0..4 {
            cb.record_failure();
            assert_eq!(cb.state(), CircuitState::Closed);
        }
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }
}
