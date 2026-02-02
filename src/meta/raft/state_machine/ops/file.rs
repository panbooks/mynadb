//! File Operations for MetaStateMachine
//!
//! Handles file registration and removal.

use crate::meta::types::{keys, Event, EventType, FileInfo, MetaResponse};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // File Operations
    // ========================================================================

    pub(crate) fn register_files(&self, files: Vec<FileInfo>) -> MetaResponse {
        let mut registered = Vec::new();

        for file in files {
            let key = file.storage_key();
            let _ = self
                .db
                .insert(key.as_bytes(), bincode::serialize(&file).unwrap());
            registered.push(file.path.clone());
        }

        // Publish event
        let payload = serde_json::to_vec(&registered).unwrap_or_default();
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::FilesRegistered,
            "files".to_string(),
            payload,
            "system".to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::EventPublished { sequence: seq }
    }

    pub(crate) fn remove_files(&self, paths: Vec<String>) -> MetaResponse {
        for path in &paths {
            // Find and remove the file entry
            let prefix = keys::PREFIX_FILE;
            for item in self.db.scan_prefix(prefix) {
                if let Ok((k, v)) = item {
                    if let Ok(file) = bincode::deserialize::<FileInfo>(&v) {
                        if file.path == *path {
                            let _ = self.db.remove(k);
                            break;
                        }
                    }
                }
            }
        }

        // Publish event
        let payload = serde_json::to_vec(&paths).unwrap_or_default();
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::FilesRemoved,
            "files".to_string(),
            payload,
            "system".to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::EventPublished { sequence: seq }
    }
}
