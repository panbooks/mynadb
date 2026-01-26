//! Circuit Breaker Pattern Implementation
//!
//! Prevents cascading failures by tracking consecutive failures per node
//! and temporarily blocking requests to unhealthy nodes.

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU8, Ordering};
use tracing::{debug, info, warn};

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
/// After enough time passes, allows a test request to check recovery.
pub struct CircuitBreaker {
    /// Consecutive failure count
    failures: AtomicU32,
    /// Timestamp of last failure (ms since epoch)
    last_failure_ms: AtomicI64,
    /// Current state: 0=Closed, 1=Open, 2=HalfOpen
    state: AtomicU8,
}

impl CircuitBreaker {
    /// Number of consecutive failures before opening circuit
    const FAILURE_THRESHOLD: u32 = 3;
    /// Time to wait before allowing a test request (10 seconds)
    const OPEN_DURATION_MS: i64 = 10_000;

    pub fn new() -> Self {
        Self {
            failures: AtomicU32::new(0),
            last_failure_ms: AtomicI64::new(0),
            state: AtomicU8::new(CircuitState::Closed as u8),
        }
    }

    /// Record a failure
    pub fn record_failure(&self) {
        let failures = self.failures.fetch_add(1, Ordering::SeqCst) + 1;
        self.last_failure_ms
            .store(current_timestamp_ms(), Ordering::SeqCst);

        if failures >= Self::FAILURE_THRESHOLD {
            let old_state = self.state.swap(CircuitState::Open as u8, Ordering::SeqCst);
            if old_state != CircuitState::Open as u8 {
                warn!("Circuit breaker opened after {} failures", failures);
            }
        }
    }

    /// Record a success - resets the circuit breaker
    pub fn record_success(&self) {
        self.failures.store(0, Ordering::SeqCst);
        let old_state = self.state.swap(CircuitState::Closed as u8, Ordering::SeqCst);
        if old_state != CircuitState::Closed as u8 {
            info!("Circuit breaker closed after successful request");
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
                if elapsed > Self::OPEN_DURATION_MS {
                    // Transition to half-open, allow one test request
                    self.state
                        .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
                    debug!("Circuit breaker transitioning to half-open");
                    true
                } else {
                    false
                }
            }
            s if s == CircuitState::HalfOpen as u8 => {
                // Allow request in half-open state (testing recovery)
                true
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
