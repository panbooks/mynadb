//! Leader Election Operations for MetaStateMachine
//!
//! Handles leader election campaign and resignation.

use crate::meta::types::{keys, EventType, LeaderInfo, MetaResponse};

use super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Leader Election
    // ========================================================================

    pub(crate) fn campaign(&self, election: &str, node_id: &str, ttl_ms: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LEADER, election);

        // Check if there's an existing leader
        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LeaderInfo>(&data) {
                if !existing.is_expired() && existing.leader != node_id {
                    return MetaResponse::LeaderExists {
                        current_leader: existing.leader,
                        term: existing.term,
                    };
                }
            }
        }

        // Become leader
        let term = self.next_fencing_token();
        let leader = LeaderInfo::new(election.to_string(), node_id.to_string(), term, ttl_ms);
        let _ = self
            .db
            .insert(storage_key.as_bytes(), bincode::serialize(&leader).unwrap());

        // Publish event
        let payload = serde_json::json!({
            "election": election,
            "leader": node_id,
            "term": term
        });
        self.publish_event(
            "leaders",
            EventType::LeaderChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::LeaderElected { term }
    }

    pub(crate) fn resign(&self, election: &str, node_id: &str, term: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LEADER, election);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LeaderInfo>(&data) {
                if existing.leader == node_id && existing.term == term {
                    let _ = self.db.remove(storage_key.as_bytes());

                    // Publish event
                    let payload = serde_json::json!({
                        "election": election,
                        "resigned": node_id,
                        "term": term
                    });
                    self.publish_event(
                        "leaders",
                        EventType::LeaderChanged,
                        serde_json::to_vec(&payload).unwrap_or_default(),
                        node_id,
                    );

                    return MetaResponse::Success;
                }
            }
        }

        MetaResponse::Error {
            message: "Not the current leader or term mismatch".to_string(),
        }
    }
}
