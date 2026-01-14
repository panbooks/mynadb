//! Tests for HA (High Availability) module
//!
//! Tests cover:
//! - State management (AtomicRole, AtomicState, HaState)
//! - Protocol messages (WireMessage, ReplicatedWalEntry)
//! - HaManager lifecycle and role management
//! - Promotion logic

#[cfg(test)]
mod state_tests {
    use crate::config::HaRole;
    use crate::ha::state::{AtomicRole, AtomicState, HaState};

    #[test]
    fn test_ha_state_from_u8() {
        assert_eq!(HaState::from(0), HaState::Initializing);
        assert_eq!(HaState::from(1), HaState::Running);
        assert_eq!(HaState::from(2), HaState::Syncing);
        assert_eq!(HaState::from(3), HaState::Promoting);
        assert_eq!(HaState::from(4), HaState::Demoting);
        assert_eq!(HaState::from(5), HaState::Failed);
        // Unknown values should map to Failed
        assert_eq!(HaState::from(255), HaState::Failed);
    }

    #[test]
    fn test_atomic_role_master() {
        let role = AtomicRole::new(HaRole::Master);
        assert_eq!(role.load(), HaRole::Master);
        assert!(role.is_master());
        assert!(!role.is_slave());
    }

    #[test]
    fn test_atomic_role_slave() {
        let role = AtomicRole::new(HaRole::Slave);
        assert_eq!(role.load(), HaRole::Slave);
        assert!(role.is_slave());
        assert!(!role.is_master());
    }

    #[test]
    fn test_atomic_role_store() {
        let role = AtomicRole::new(HaRole::Master);
        assert!(role.is_master());

        role.store(HaRole::Slave);
        assert!(role.is_slave());

        role.store(HaRole::Master);
        assert!(role.is_master());
    }

    #[test]
    fn test_atomic_state_store_and_load() {
        let state = AtomicState::new(HaState::Initializing);
        assert_eq!(state.load(), HaState::Initializing);

        state.store(HaState::Running);
        assert_eq!(state.load(), HaState::Running);

        state.store(HaState::Syncing);
        assert_eq!(state.load(), HaState::Syncing);

        state.store(HaState::Promoting);
        assert_eq!(state.load(), HaState::Promoting);

        state.store(HaState::Demoting);
        assert_eq!(state.load(), HaState::Demoting);

        state.store(HaState::Failed);
        assert_eq!(state.load(), HaState::Failed);
    }

    #[test]
    fn test_atomic_role_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let role = Arc::new(AtomicRole::new(HaRole::Slave));
        let mut handles = vec![];

        // Spawn multiple threads to read role
        for _ in 0..10 {
            let role_clone = role.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = role_clone.load();
                }
            }));
        }

        // Spawn thread to toggle role
        let role_clone = role.clone();
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                if i % 2 == 0 {
                    role_clone.store(HaRole::Master);
                } else {
                    role_clone.store(HaRole::Slave);
                }
            }
        }));

        for handle in handles {
            handle.join().unwrap();
        }

        // Final state should be one of the valid roles
        let final_role = role.load();
        assert!(final_role == HaRole::Master || final_role == HaRole::Slave);
    }
}

#[cfg(test)]
mod protocol_tests {
    use crate::ha::replication::protocol::*;

    #[test]
    fn test_replicated_wal_entry_checksum() {
        let entry = ReplicatedWalEntry {
            sequence: 1,
            timestamp_nanos: 1000000000,
            entry_type: 0,
            db_name: "testdb".to_string(),
            payload: b"cpu,host=server1 value=42.0".to_vec(),
            checksum: 0,
        };

        let checksum = entry.calculate_checksum();
        assert!(checksum != 0);

        // Same data should produce same checksum
        let entry2 = ReplicatedWalEntry {
            sequence: 1,
            timestamp_nanos: 1000000000,
            entry_type: 0,
            db_name: "testdb".to_string(),
            payload: b"cpu,host=server1 value=42.0".to_vec(),
            checksum: 0,
        };
        assert_eq!(checksum, entry2.calculate_checksum());
    }

    #[test]
    fn test_replicated_wal_entry_checksum_different_data() {
        let entry1 = ReplicatedWalEntry {
            sequence: 1,
            timestamp_nanos: 1000000000,
            entry_type: 0,
            db_name: "testdb".to_string(),
            payload: b"cpu,host=server1 value=42.0".to_vec(),
            checksum: 0,
        };

        let entry2 = ReplicatedWalEntry {
            sequence: 2, // Different sequence
            timestamp_nanos: 1000000000,
            entry_type: 0,
            db_name: "testdb".to_string(),
            payload: b"cpu,host=server1 value=42.0".to_vec(),
            checksum: 0,
        };

        assert_ne!(entry1.calculate_checksum(), entry2.calculate_checksum());
    }

    #[test]
    fn test_replicated_wal_entry_verify_checksum() {
        let mut entry = ReplicatedWalEntry {
            sequence: 1,
            timestamp_nanos: 1000000000,
            entry_type: 0,
            db_name: "testdb".to_string(),
            payload: b"cpu,host=server1 value=42.0".to_vec(),
            checksum: 0,
        };

        // Without correct checksum
        assert!(!entry.verify_checksum());

        // Set correct checksum
        entry.checksum = entry.calculate_checksum();
        assert!(entry.verify_checksum());

        // Corrupt the checksum
        entry.checksum = entry.checksum.wrapping_add(1);
        assert!(!entry.verify_checksum());
    }

    #[test]
    fn test_wire_message_encode_decode() {
        let handshake = HandshakeRequest {
            node_id: "slave-01".to_string(),
            last_sequence: 100,
            protocol_version: PROTOCOL_VERSION,
        };

        let wire_msg = WireMessage::new(MessageType::Handshake, &handshake).unwrap();
        let encoded = wire_msg.encode().unwrap();
        let decoded = WireMessage::decode(&encoded).unwrap();

        assert_eq!(decoded.msg_type, MessageType::Handshake);

        let decoded_handshake: HandshakeRequest = decoded.decode_payload().unwrap();
        assert_eq!(decoded_handshake.node_id, "slave-01");
        assert_eq!(decoded_handshake.last_sequence, 100);
        assert_eq!(decoded_handshake.protocol_version, PROTOCOL_VERSION);
    }

    #[test]
    fn test_wire_message_wal_entry() {
        let entry = ReplicatedWalEntry {
            sequence: 42,
            timestamp_nanos: 1234567890,
            entry_type: 0,
            db_name: "mydb".to_string(),
            payload: b"test data".to_vec(),
            checksum: 12345,
        };

        let wire_msg = WireMessage::new(MessageType::WalEntry, &entry).unwrap();
        let encoded = wire_msg.encode().unwrap();
        let decoded = WireMessage::decode(&encoded).unwrap();

        assert_eq!(decoded.msg_type, MessageType::WalEntry);

        let decoded_entry: ReplicatedWalEntry = decoded.decode_payload().unwrap();
        assert_eq!(decoded_entry.sequence, 42);
        assert_eq!(decoded_entry.db_name, "mydb");
        assert_eq!(decoded_entry.payload, b"test data");
    }

    #[test]
    fn test_wire_message_heartbeat() {
        let heartbeat = HeartbeatMessage {
            node_id: "master-01".to_string(),
            last_sequence: 999,
            timestamp_nanos: 9999999999,
        };

        let wire_msg = WireMessage::new(MessageType::Heartbeat, &heartbeat).unwrap();
        let encoded = wire_msg.encode().unwrap();
        let decoded = WireMessage::decode(&encoded).unwrap();

        assert_eq!(decoded.msg_type, MessageType::Heartbeat);

        let decoded_hb: HeartbeatMessage = decoded.decode_payload().unwrap();
        assert_eq!(decoded_hb.node_id, "master-01");
        assert_eq!(decoded_hb.last_sequence, 999);
    }

    #[test]
    fn test_wire_message_ack() {
        let ack = AckMessage {
            sequence: 50,
            node_id: "slave-02".to_string(),
            success: true,
            error: None,
        };

        let wire_msg = WireMessage::new(MessageType::Ack, &ack).unwrap();
        let decoded = WireMessage::decode(&wire_msg.encode().unwrap()).unwrap();
        let decoded_ack: AckMessage = decoded.decode_payload().unwrap();

        assert_eq!(decoded_ack.sequence, 50);
        assert!(decoded_ack.success);
        assert!(decoded_ack.error.is_none());
    }

    #[test]
    fn test_wire_message_ack_with_error() {
        let ack = AckMessage {
            sequence: 51,
            node_id: "slave-02".to_string(),
            success: false,
            error: Some("Checksum mismatch".to_string()),
        };

        let wire_msg = WireMessage::new(MessageType::Ack, &ack).unwrap();
        let decoded = WireMessage::decode(&wire_msg.encode().unwrap()).unwrap();
        let decoded_ack: AckMessage = decoded.decode_payload().unwrap();

        assert_eq!(decoded_ack.sequence, 51);
        assert!(!decoded_ack.success);
        assert_eq!(decoded_ack.error, Some("Checksum mismatch".to_string()));
    }

    #[test]
    fn test_handshake_response() {
        let response = HandshakeResponse {
            node_id: "master-01".to_string(),
            accepted: true,
            current_sequence: 12345,
            error: None,
        };

        let wire_msg = WireMessage::new(MessageType::HandshakeResponse, &response).unwrap();
        let decoded = WireMessage::decode(&wire_msg.encode().unwrap()).unwrap();
        let decoded_resp: HandshakeResponse = decoded.decode_payload().unwrap();

        assert_eq!(decoded_resp.node_id, "master-01");
        assert!(decoded_resp.accepted);
        assert_eq!(decoded_resp.current_sequence, 12345);
    }

    #[test]
    fn test_handshake_response_rejected() {
        let response = HandshakeResponse {
            node_id: "master-01".to_string(),
            accepted: false,
            current_sequence: 0,
            error: Some("Protocol version mismatch".to_string()),
        };

        let wire_msg = WireMessage::new(MessageType::HandshakeResponse, &response).unwrap();
        let decoded = WireMessage::decode(&wire_msg.encode().unwrap()).unwrap();
        let decoded_resp: HandshakeResponse = decoded.decode_payload().unwrap();

        assert!(!decoded_resp.accepted);
        assert_eq!(
            decoded_resp.error,
            Some("Protocol version mismatch".to_string())
        );
    }

    #[test]
    fn test_message_type_values() {
        assert_eq!(MessageType::Handshake as u8, 0);
        assert_eq!(MessageType::HandshakeResponse as u8, 1);
        assert_eq!(MessageType::WalEntry as u8, 2);
        assert_eq!(MessageType::Ack as u8, 3);
        assert_eq!(MessageType::Heartbeat as u8, 4);
        assert_eq!(MessageType::HeartbeatResponse as u8, 5);
    }

    #[test]
    fn test_protocol_version() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }

    #[test]
    fn test_max_message_size() {
        assert_eq!(MAX_MESSAGE_SIZE, 16 * 1024 * 1024);
    }
}

#[cfg(test)]
mod ha_manager_tests {
    use crate::config::{
        FailoverConfig, FailoverMode, HaConfig, HaRole, HeartbeatConfig, ReplicationConfig,
    };
    use crate::ha::state::HaState;
    use crate::ha::HaManager;

    fn create_disabled_config() -> HaConfig {
        HaConfig {
            enabled: false,
            role: HaRole::Master,
            node_id: "test-node".to_string(),
            replication: ReplicationConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            failover: FailoverConfig::default(),
        }
    }

    fn create_master_config() -> HaConfig {
        HaConfig {
            enabled: true,
            role: HaRole::Master,
            node_id: "master-01".to_string(),
            replication: ReplicationConfig {
                listen_addr: "127.0.0.1:17003".to_string(),
                ..Default::default()
            },
            heartbeat: HeartbeatConfig::default(),
            failover: FailoverConfig::default(),
        }
    }

    fn create_slave_config() -> HaConfig {
        HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: ReplicationConfig {
                master_addr: Some("127.0.0.1:17003".to_string()),
                ..Default::default()
            },
            heartbeat: HeartbeatConfig::default(),
            failover: FailoverConfig {
                mode: FailoverMode::Manual,
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_ha_manager_disabled() {
        let config = create_disabled_config();
        let manager = HaManager::new(&config).unwrap();

        assert!(!manager.is_enabled());
        assert!(manager.is_master()); // Default role is master
    }

    #[test]
    fn test_ha_manager_master() {
        let config = create_master_config();
        let manager = HaManager::new(&config).unwrap();

        assert!(manager.is_enabled());
        assert!(manager.is_master());
        assert!(!manager.is_slave());
        assert_eq!(manager.role(), HaRole::Master);
        assert_eq!(manager.node_id(), "master-01");
    }

    #[test]
    fn test_ha_manager_slave() {
        let config = create_slave_config();
        let manager = HaManager::new(&config).unwrap();

        assert!(manager.is_enabled());
        assert!(manager.is_slave());
        assert!(!manager.is_master());
        assert_eq!(manager.role(), HaRole::Slave);
        assert_eq!(manager.node_id(), "slave-01");
    }

    #[test]
    fn test_ha_manager_initial_state() {
        let config = create_master_config();
        let manager = HaManager::new(&config).unwrap();

        // Enabled HA starts in Initializing state
        assert_eq!(manager.state(), HaState::Initializing);
    }

    #[test]
    fn test_ha_manager_disabled_initial_state() {
        let config = create_disabled_config();
        let manager = HaManager::new(&config).unwrap();

        // Disabled HA starts in Running state
        assert_eq!(manager.state(), HaState::Running);
    }

    #[test]
    fn test_ha_manager_master_has_sender() {
        let config = create_master_config();
        let manager = HaManager::new(&config).unwrap();

        assert!(manager.replication().is_some());
        assert!(manager.replication().unwrap().sender().is_some());
    }

    #[test]
    fn test_ha_manager_slave_has_receiver() {
        let config = create_slave_config();
        let manager = HaManager::new(&config).unwrap();

        assert!(manager.replication().is_some());
        assert!(manager.replication().unwrap().receiver().is_some());
    }

    #[test]
    fn test_ha_manager_take_entry_receiver() {
        let config = create_slave_config();
        let mut manager = HaManager::new(&config).unwrap();

        // First take should succeed
        let rx = manager.take_entry_receiver();
        assert!(rx.is_some());

        // Second take should return None
        let rx2 = manager.take_entry_receiver();
        assert!(rx2.is_none());
    }

    #[test]
    fn test_ha_manager_master_no_entry_receiver() {
        let config = create_master_config();
        let mut manager = HaManager::new(&config).unwrap();

        // Master should not have entry receiver
        let rx = manager.take_entry_receiver();
        assert!(rx.is_none());
    }

    #[tokio::test]
    async fn test_ha_manager_replicate_write_not_master() {
        let config = create_slave_config();
        let manager = HaManager::new(&config).unwrap();

        let result = manager.replicate_write("testdb", b"test data", 0).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not master"));
    }

    #[tokio::test]
    async fn test_ha_manager_status_disabled() {
        let config = create_disabled_config();
        let manager = HaManager::new(&config).unwrap();

        let status = manager.status().await;
        assert!(!status.enabled);
        assert_eq!(status.node_id, "test-node");
    }

    #[tokio::test]
    async fn test_ha_manager_status_enabled() {
        let config = create_master_config();
        let manager = HaManager::new(&config).unwrap();

        let status = manager.status().await;
        assert!(status.enabled);
        assert_eq!(status.node_id, "master-01");
        assert_eq!(status.role, HaRole::Master);
    }
}

#[cfg(test)]
mod promoter_tests {
    use std::sync::Arc;

    use crate::config::{FailoverConfig, FailoverMode, HaConfig, HaRole};
    use crate::ha::failover::promoter::{PromotionResult, Promoter};
    use crate::ha::state::{AtomicRole, AtomicState, HaState};

    fn create_promoter(role: HaRole, state: HaState) -> Promoter {
        let config = HaConfig {
            enabled: true,
            role,
            node_id: "test-node".to_string(),
            replication: Default::default(),
            heartbeat: Default::default(),
            failover: FailoverConfig {
                mode: FailoverMode::Manual,
                ..Default::default()
            },
        };

        let role_atomic = Arc::new(AtomicRole::new(role));
        let state_atomic = Arc::new(AtomicState::new(state));

        let (promoter, _rx) = Promoter::new(&config, role_atomic, state_atomic);
        promoter
    }

    #[test]
    fn test_can_promote_slave_running() {
        let promoter = create_promoter(HaRole::Slave, HaState::Running);
        assert!(promoter.can_promote());
    }

    #[test]
    fn test_can_promote_slave_syncing() {
        let promoter = create_promoter(HaRole::Slave, HaState::Syncing);
        assert!(promoter.can_promote());
    }

    #[test]
    fn test_cannot_promote_master() {
        let promoter = create_promoter(HaRole::Master, HaState::Running);
        assert!(!promoter.can_promote());
    }

    #[test]
    fn test_cannot_promote_initializing() {
        let promoter = create_promoter(HaRole::Slave, HaState::Initializing);
        assert!(!promoter.can_promote());
    }

    #[test]
    fn test_cannot_promote_promoting() {
        let promoter = create_promoter(HaRole::Slave, HaState::Promoting);
        assert!(!promoter.can_promote());
    }

    #[test]
    fn test_cannot_promote_failed() {
        let promoter = create_promoter(HaRole::Slave, HaState::Failed);
        assert!(!promoter.can_promote());
    }

    #[tokio::test]
    async fn test_promote_slave_to_master() {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: Default::default(),
            heartbeat: Default::default(),
            failover: FailoverConfig {
                mode: FailoverMode::Manual,
                ..Default::default()
            },
        };

        let role = Arc::new(AtomicRole::new(HaRole::Slave));
        let state = Arc::new(AtomicState::new(HaState::Running));

        let (promoter, _rx) = Promoter::new(&config, role.clone(), state.clone());

        // Before promotion
        assert!(role.is_slave());

        // Promote
        let result = promoter.promote().await;
        assert!(matches!(result, PromotionResult::Success));

        // After promotion
        assert!(role.is_master());
        assert_eq!(state.load(), HaState::Running);
    }

    #[tokio::test]
    async fn test_promote_already_master() {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Master,
            node_id: "master-01".to_string(),
            replication: Default::default(),
            heartbeat: Default::default(),
            failover: Default::default(),
        };

        let role = Arc::new(AtomicRole::new(HaRole::Master));
        let state = Arc::new(AtomicState::new(HaState::Running));

        let (promoter, _rx) = Promoter::new(&config, role.clone(), state.clone());

        let result = promoter.promote().await;
        assert!(matches!(result, PromotionResult::AlreadyMaster));
    }

    #[tokio::test]
    async fn test_promote_wrong_state() {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: Default::default(),
            heartbeat: Default::default(),
            failover: Default::default(),
        };

        let role = Arc::new(AtomicRole::new(HaRole::Slave));
        let state = Arc::new(AtomicState::new(HaState::Initializing));

        let (promoter, _rx) = Promoter::new(&config, role.clone(), state.clone());

        let result = promoter.promote().await;
        assert!(matches!(result, PromotionResult::NotAllowed(_)));
    }

    #[test]
    fn test_state_info() {
        let promoter = create_promoter(HaRole::Slave, HaState::Running);
        let info = promoter.state_info();

        assert_eq!(info.role, HaRole::Slave);
        assert_eq!(info.state, HaState::Running);
        assert!(info.can_promote);
        assert_eq!(info.failover_mode, FailoverMode::Manual);
    }

    #[tokio::test]
    async fn test_demote_master_to_slave() {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Master,
            node_id: "master-01".to_string(),
            replication: Default::default(),
            heartbeat: Default::default(),
            failover: Default::default(),
        };

        let role = Arc::new(AtomicRole::new(HaRole::Master));
        let state = Arc::new(AtomicState::new(HaState::Running));

        let (promoter, _rx) = Promoter::new(&config, role.clone(), state.clone());

        // Before demotion
        assert!(role.is_master());

        // Demote
        let result = promoter.demote("127.0.0.1:7003").await;
        assert!(result.is_ok());

        // After demotion
        assert!(role.is_slave());
        assert_eq!(state.load(), HaState::Syncing);
    }

    #[tokio::test]
    async fn test_demote_already_slave() {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: Default::default(),
            heartbeat: Default::default(),
            failover: Default::default(),
        };

        let role = Arc::new(AtomicRole::new(HaRole::Slave));
        let state = Arc::new(AtomicState::new(HaState::Running));

        let (promoter, _rx) = Promoter::new(&config, role.clone(), state.clone());

        // Should succeed without changing anything
        let result = promoter.demote("127.0.0.1:7003").await;
        assert!(result.is_ok());
        assert!(role.is_slave());
    }
}

#[cfg(test)]
mod replication_manager_tests {
    use crate::config::{HaConfig, HaRole, ReplicationConfig};
    use crate::ha::replication::ReplicationManager;

    fn create_master_config() -> HaConfig {
        HaConfig {
            enabled: true,
            role: HaRole::Master,
            node_id: "master-01".to_string(),
            replication: ReplicationConfig {
                listen_addr: "127.0.0.1:18003".to_string(),
                ..Default::default()
            },
            heartbeat: Default::default(),
            failover: Default::default(),
        }
    }

    fn create_slave_config() -> HaConfig {
        HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: ReplicationConfig {
                master_addr: Some("127.0.0.1:18003".to_string()),
                ..Default::default()
            },
            heartbeat: Default::default(),
            failover: Default::default(),
        }
    }

    #[test]
    fn test_replication_manager_master() {
        let config = create_master_config();
        let manager = ReplicationManager::new(&config).unwrap();

        assert!(manager.is_master());
        assert!(!manager.is_slave());
        assert_eq!(manager.role(), HaRole::Master);
        assert!(manager.sender().is_some());
        assert!(manager.receiver().is_none());
    }

    #[test]
    fn test_replication_manager_slave() {
        let config = create_slave_config();
        let manager = ReplicationManager::new(&config).unwrap();

        assert!(manager.is_slave());
        assert!(!manager.is_master());
        assert_eq!(manager.role(), HaRole::Slave);
        assert!(manager.sender().is_none());
        assert!(manager.receiver().is_some());
    }

    #[test]
    fn test_replication_manager_take_entry_receiver() {
        let config = create_slave_config();
        let mut manager = ReplicationManager::new(&config).unwrap();

        // Slave should have entry receiver
        let rx = manager.take_entry_receiver();
        assert!(rx.is_some());

        // Second take should return None
        let rx2 = manager.take_entry_receiver();
        assert!(rx2.is_none());
    }

    #[test]
    fn test_replication_manager_master_no_entry_receiver() {
        let config = create_master_config();
        let mut manager = ReplicationManager::new(&config).unwrap();

        // Master should not have entry receiver
        let rx = manager.take_entry_receiver();
        assert!(rx.is_none());
    }

    #[tokio::test]
    async fn test_replication_stats_master() {
        let config = create_master_config();
        let manager = ReplicationManager::new(&config).unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.role, HaRole::Master);
        assert!(stats.connected);
        assert!(stats.slave_count.is_some());
    }

    #[tokio::test]
    async fn test_replication_stats_slave() {
        let config = create_slave_config();
        let manager = ReplicationManager::new(&config).unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.role, HaRole::Slave);
        assert!(stats.slave_count.is_none());
        assert!(stats.seconds_since_heartbeat.is_some());
    }
}

#[cfg(test)]
mod failover_manager_tests {
    use std::sync::Arc;

    use crate::config::{FailoverConfig, FailoverMode, HaConfig, HaRole, ReplicationConfig};
    use crate::ha::failover::{FailoverManager, PromotionResult};
    use crate::ha::replication::ReplicationManager;
    use crate::ha::state::{AtomicRole, AtomicState, HaState};

    fn create_slave_failover_manager() -> FailoverManager {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: ReplicationConfig {
                master_addr: Some("127.0.0.1:19003".to_string()),
                ..Default::default()
            },
            heartbeat: Default::default(),
            failover: FailoverConfig {
                mode: FailoverMode::Manual,
                failure_threshold: 3,
                promotion_delay_secs: 2,
            },
        };

        let role = Arc::new(AtomicRole::new(HaRole::Slave));
        let state = Arc::new(AtomicState::new(HaState::Running));

        let repl_manager = ReplicationManager::new(&config).unwrap();
        let receiver = repl_manager.receiver();

        FailoverManager::new(&config, role, state, receiver)
    }

    fn create_master_failover_manager() -> FailoverManager {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Master,
            node_id: "master-01".to_string(),
            replication: ReplicationConfig {
                listen_addr: "127.0.0.1:19003".to_string(),
                ..Default::default()
            },
            heartbeat: Default::default(),
            failover: FailoverConfig {
                mode: FailoverMode::Manual,
                ..Default::default()
            },
        };

        let role = Arc::new(AtomicRole::new(HaRole::Master));
        let state = Arc::new(AtomicState::new(HaState::Running));

        FailoverManager::new(&config, role, state, None)
    }

    #[test]
    fn test_failover_manager_slave_has_detector() {
        let fm = create_slave_failover_manager();
        assert!(fm.detector().is_some());
    }

    #[test]
    fn test_failover_manager_master_no_detector() {
        let fm = create_master_failover_manager();
        assert!(fm.detector().is_none());
    }

    #[test]
    fn test_failover_manager_has_promoter() {
        let fm = create_slave_failover_manager();
        assert!(fm.promoter().can_promote());
    }

    #[tokio::test]
    async fn test_manual_promote_slave() {
        let fm = create_slave_failover_manager();
        let result = fm.manual_promote().await;
        assert!(matches!(result, PromotionResult::Success));
    }

    #[tokio::test]
    async fn test_manual_promote_disabled() {
        let config = HaConfig {
            enabled: true,
            role: HaRole::Slave,
            node_id: "slave-01".to_string(),
            replication: ReplicationConfig {
                master_addr: Some("127.0.0.1:19004".to_string()),
                ..Default::default()
            },
            heartbeat: Default::default(),
            failover: FailoverConfig {
                mode: FailoverMode::Disabled, // Failover disabled
                ..Default::default()
            },
        };

        let role = Arc::new(AtomicRole::new(HaRole::Slave));
        let state = Arc::new(AtomicState::new(HaState::Running));

        let repl_manager = ReplicationManager::new(&config).unwrap();
        let receiver = repl_manager.receiver();

        let fm = FailoverManager::new(&config, role, state, receiver);

        let result = fm.manual_promote().await;
        assert!(matches!(result, PromotionResult::NotAllowed(_)));
    }
}
