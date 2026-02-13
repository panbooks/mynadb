use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};

/// Token bucket rate limiter for I/O throttling
/// Limits bytes per second to prevent I/O saturation
#[derive(Clone)]
pub struct RateLimiter {
    /// Maximum bytes per second (0 = unlimited)
    bytes_per_sec: u64,
    /// Current available tokens (bytes)
    tokens: Arc<Mutex<f64>>,
    /// Maximum burst size (bytes)
    max_tokens: f64,
    /// Last refill time
    last_refill: Arc<Mutex<Instant>>,
}

impl RateLimiter {
    /// Create a new rate limiter
    ///
    /// # Arguments
    /// * `bytes_per_sec` - Maximum bytes per second (0 = unlimited)
    pub fn new(bytes_per_sec: u64) -> Self {
        let max_tokens = if bytes_per_sec > 0 {
            bytes_per_sec as f64
        } else {
            f64::MAX
        };

        RateLimiter {
            bytes_per_sec,
            tokens: Arc::new(Mutex::new(max_tokens)),
            max_tokens,
            last_refill: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Check if rate limiting is enabled
    pub fn is_enabled(&self) -> bool {
        self.bytes_per_sec > 0
    }

    /// Wait for permission to consume the specified number of bytes
    /// This will block (sleep) if necessary to maintain the rate limit
    pub async fn wait_for_bytes(&self, bytes: usize) {
        if !self.is_enabled() || bytes == 0 {
            return;
        }

        let bytes = bytes as f64;

        loop {
            // Refill tokens based on elapsed time
            self.refill_tokens().await;

            let mut tokens = self.tokens.lock().await;

            if *tokens >= bytes {
                // Consume tokens
                *tokens -= bytes;
                break;
            } else {
                // Not enough tokens, calculate wait time
                let deficit = bytes - *tokens;
                let wait_secs = deficit / self.bytes_per_sec as f64;
                drop(tokens); // Release lock before sleeping

                // Sleep for the calculated time
                sleep(Duration::from_secs_f64(wait_secs)).await;
            }
        }
    }

    /// Refill tokens based on elapsed time since last refill
    async fn refill_tokens(&self) {
        let mut last_refill = self.last_refill.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        if elapsed.as_secs_f64() > 0.0 {
            let mut tokens = self.tokens.lock().await;

            // Calculate tokens to add based on elapsed time
            let tokens_to_add = elapsed.as_secs_f64() * self.bytes_per_sec as f64;
            *tokens = (*tokens + tokens_to_add).min(self.max_tokens);

            *last_refill = now;
        }
    }

    /// Get current available tokens (for monitoring)
    #[cfg(test)]
    pub async fn available_tokens(&self) -> f64 {
        *self.tokens.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unlimited_rate_limiter() {
        let limiter = RateLimiter::new(0);
        assert!(!limiter.is_enabled());

        // Should not block
        limiter.wait_for_bytes(1_000_000).await;
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        // 100 KB/s
        let limiter = RateLimiter::new(100_000);
        assert!(limiter.is_enabled());

        let start = Instant::now();

        // Consume 100KB (should be instant, within burst)
        limiter.wait_for_bytes(100_000).await;

        // Consume another 100KB (should wait ~1 second)
        limiter.wait_for_bytes(100_000).await;

        let elapsed = start.elapsed();

        // Should take approximately 1 second (with some tolerance)
        assert!(elapsed.as_secs_f64() >= 0.9 && elapsed.as_secs_f64() <= 1.5);
    }

    #[tokio::test]
    async fn test_token_refill() {
        let limiter = RateLimiter::new(100_000);

        // Consume all tokens
        limiter.wait_for_bytes(100_000).await;

        let tokens_before = limiter.available_tokens().await;
        assert!(tokens_before < 10_000.0); // Should be near zero

        // Wait for refill
        sleep(Duration::from_millis(500)).await;
        limiter.refill_tokens().await;

        let tokens_after = limiter.available_tokens().await;
        assert!(tokens_after > 40_000.0); // Should have refilled ~50KB
    }
}
