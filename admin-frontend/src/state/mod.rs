//! Global application state management

use admin_types::{ClusterStatus, NodeSummary, TableSummary};
use leptos::*;

/// Global application state
#[derive(Clone)]
pub struct AppState {
    /// Cluster status
    pub cluster_status: RwSignal<Option<ClusterStatus>>,
    /// List of nodes
    pub nodes: RwSignal<Vec<NodeSummary>>,
    /// List of tables
    pub tables: RwSignal<Vec<TableSummary>>,
    /// Loading state
    pub loading: RwSignal<bool>,
    /// Error message
    pub error: RwSignal<Option<String>>,
    /// WebSocket connected
    pub ws_connected: RwSignal<bool>,
    /// Toast notifications
    pub toasts: RwSignal<Vec<Toast>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            cluster_status: create_rw_signal(None),
            nodes: create_rw_signal(Vec::new()),
            tables: create_rw_signal(Vec::new()),
            loading: create_rw_signal(false),
            error: create_rw_signal(None),
            ws_connected: create_rw_signal(false),
            toasts: create_rw_signal(Vec::new()),
        }
    }

    /// Add a toast notification
    pub fn add_toast(&self, toast: Toast) {
        self.toasts.update(|t| t.push(toast));
    }

    /// Remove a toast by id
    pub fn remove_toast(&self, id: u64) {
        self.toasts.update(|t| t.retain(|toast| toast.id != id));
    }

    /// Set error and show toast
    pub fn set_error(&self, message: impl Into<String>) {
        let msg = message.into();
        self.error.set(Some(msg.clone()));
        self.add_toast(Toast::error(msg));
    }

    /// Clear error
    pub fn clear_error(&self) {
        self.error.set(None);
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

/// Toast notification
#[derive(Clone, Debug)]
pub struct Toast {
    pub id: u64,
    pub message: String,
    pub toast_type: ToastType,
    pub duration_ms: u64,
}

impl Toast {
    fn new(message: impl Into<String>, toast_type: ToastType) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self {
            id: COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            message: message.into(),
            toast_type,
            duration_ms: 5000,
        }
    }

    pub fn success(message: impl Into<String>) -> Self {
        Self::new(message, ToastType::Success)
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::new(message, ToastType::Error)
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(message, ToastType::Warning)
    }

    pub fn info(message: impl Into<String>) -> Self {
        Self::new(message, ToastType::Info)
    }
}

/// Toast type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToastType {
    Success,
    Error,
    Warning,
    Info,
}

/// Provide application state via context
pub fn provide_app_state() {
    let state = AppState::new();
    provide_context(state);
}

/// Get application state from context
pub fn use_app_state() -> AppState {
    expect_context::<AppState>()
}
