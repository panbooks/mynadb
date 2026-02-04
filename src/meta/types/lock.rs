//! Lock, leader election, and lease types

use serde::{Deserialize, Serialize};

use super::{current_timestamp_ms, keys};

/// Lock information with fencing token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    pub key: String,
    pub holder: String,
    pub fencing_token: u64,
    pub acquired_at: i64,
    pub expires_at: i64,
}

impl LockInfo {
    pub fn new(key: String, holder: String, fencing_token: u64, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            key,
            holder,
            fencing_token,
            acquired_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_LOCK, self.key)
    }
}

/// Leader election information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub election: String,
    pub leader: String,
    pub term: u64,
    pub acquired_at: i64,
    pub expires_at: i64,
}

impl LeaderInfo {
    pub fn new(election: String, leader: String, term: u64, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            election,
            leader,
            term,
            acquired_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_LEADER, self.election)
    }
}

/// Node lease for auto-deregistration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub lease_id: u64,
    pub node_id: String,
    pub ttl_ms: u64,
    pub created_at: i64,
    pub expires_at: i64,
}

impl LeaseInfo {
    pub fn new(lease_id: u64, node_id: String, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            lease_id,
            node_id,
            ttl_ms,
            created_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    pub fn renew(&mut self) {
        let now = current_timestamp_ms();
        self.expires_at = now + self.ttl_ms as i64;
    }

    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_LEASE, self.lease_id)
    }
}
