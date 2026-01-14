//! HA State management

use std::sync::atomic::{AtomicU8, Ordering};

use crate::config::HaRole;

/// HA operational state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HaState {
    /// Initializing HA components
    Initializing = 0,
    /// Normal operation
    Running = 1,
    /// Slave is syncing with master
    Syncing = 2,
    /// Slave is promoting to master
    Promoting = 3,
    /// Master is demoting to slave
    Demoting = 4,
    /// HA component failed
    Failed = 5,
}

impl From<u8> for HaState {
    fn from(v: u8) -> Self {
        match v {
            0 => HaState::Initializing,
            1 => HaState::Running,
            2 => HaState::Syncing,
            3 => HaState::Promoting,
            4 => HaState::Demoting,
            _ => HaState::Failed,
        }
    }
}

/// Atomic role holder for thread-safe role management
pub struct AtomicRole {
    role: AtomicU8,
}

impl AtomicRole {
    pub fn new(role: HaRole) -> Self {
        let value = match role {
            HaRole::Master => 0,
            HaRole::Slave => 1,
        };
        Self {
            role: AtomicU8::new(value),
        }
    }

    pub fn load(&self) -> HaRole {
        match self.role.load(Ordering::SeqCst) {
            0 => HaRole::Master,
            _ => HaRole::Slave,
        }
    }

    pub fn store(&self, role: HaRole) {
        let value = match role {
            HaRole::Master => 0,
            HaRole::Slave => 1,
        };
        self.role.store(value, Ordering::SeqCst);
    }

    pub fn is_master(&self) -> bool {
        self.load() == HaRole::Master
    }

    pub fn is_slave(&self) -> bool {
        self.load() == HaRole::Slave
    }
}

/// Atomic state holder
pub struct AtomicState {
    state: AtomicU8,
}

impl AtomicState {
    pub fn new(state: HaState) -> Self {
        Self {
            state: AtomicU8::new(state as u8),
        }
    }

    pub fn load(&self) -> HaState {
        HaState::from(self.state.load(Ordering::SeqCst))
    }

    pub fn store(&self, state: HaState) {
        self.state.store(state as u8, Ordering::SeqCst);
    }
}
