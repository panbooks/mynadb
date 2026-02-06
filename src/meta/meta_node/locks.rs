//! Lock operations

use super::MetaNode;
use crate::meta::types::{MetaCommand, MetaResponse};

impl MetaNode {
    /// Try to acquire a lock
    pub async fn try_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<(bool, u64), String> {
        let cmd = MetaCommand::AcquireLock {
            key: key.to_string(),
            holder: holder.to_string(),
            ttl_ms,
        };

        match self.apply_command(cmd).await? {
            MetaResponse::LockAcquired { fencing_token } => Ok((true, fencing_token)),
            MetaResponse::LockNotAcquired { .. } => Ok((false, 0)),
            MetaResponse::Error { message } => Err(message),
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }

    /// Release a lock
    pub async fn unlock(&self, key: &str, holder: &str, fencing_token: u64) -> Result<(), String> {
        let cmd = MetaCommand::ReleaseLock {
            key: key.to_string(),
            holder: holder.to_string(),
            fencing_token,
        };
        self.apply_command(cmd).await?;
        Ok(())
    }

    /// Renew a lock
    pub async fn renew_lock(
        &self,
        key: &str,
        holder: &str,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> Result<i64, String> {
        let cmd = MetaCommand::RenewLock {
            key: key.to_string(),
            holder: holder.to_string(),
            fencing_token,
            ttl_ms,
        };

        match self.apply_command(cmd).await? {
            MetaResponse::LockRenewed { expires_at } => Ok(expires_at),
            MetaResponse::Error { message } => Err(message),
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }
}
