//! Lock Operations for MetaStateMachine
//!
//! Handles distributed lock acquisition, release, and renewal.

use crate::meta::types::{current_timestamp_ms, keys, LockInfo, MetaResponse};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Lock Operations
    // ========================================================================

    pub(crate) fn acquire_lock(&self, key: &str, holder: &str, ttl_ms: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);

        // Check if lock exists and is not expired
        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LockInfo>(&data) {
                if !existing.is_expired() && existing.holder != holder {
                    return MetaResponse::LockNotAcquired {
                        current_holder: existing.holder,
                        expires_at: existing.expires_at,
                    };
                }
            }
        }

        // Acquire the lock
        let fencing_token = self.next_fencing_token();
        let lock = LockInfo::new(key.to_string(), holder.to_string(), fencing_token, ttl_ms);
        let _ = self
            .db
            .insert(storage_key.as_bytes(), bincode::serialize(&lock).unwrap());

        MetaResponse::LockAcquired { fencing_token }
    }

    pub(crate) fn release_lock(&self, key: &str, holder: &str, fencing_token: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LockInfo>(&data) {
                // Validate holder and fencing token
                if existing.holder == holder && existing.fencing_token == fencing_token {
                    let _ = self.db.remove(storage_key.as_bytes());
                    return MetaResponse::LockReleased;
                }
            }
        }

        MetaResponse::Error {
            message: "Lock not held or fencing token mismatch".to_string(),
        }
    }

    pub(crate) fn renew_lock(
        &self,
        key: &str,
        holder: &str,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(mut existing) = bincode::deserialize::<LockInfo>(&data) {
                if existing.holder == holder && existing.fencing_token == fencing_token {
                    let now = current_timestamp_ms();
                    existing.expires_at = now + ttl_ms as i64;
                    let _ = self.db.insert(
                        storage_key.as_bytes(),
                        bincode::serialize(&existing).unwrap(),
                    );
                    return MetaResponse::LockRenewed {
                        expires_at: existing.expires_at,
                    };
                }
            }
        }

        MetaResponse::Error {
            message: "Lock not held or fencing token mismatch".to_string(),
        }
    }
}
