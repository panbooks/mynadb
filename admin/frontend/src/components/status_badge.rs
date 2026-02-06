//! Status badge components

use admin_types::{DataNodeMode, NodeRole, NodeState};
use leptos::*;

/// Badge for node state
#[component]
pub fn StateBadge(state: NodeState) -> impl IntoView {
    let (class, text) = match state {
        NodeState::Active => ("badge-success", "Active"),
        NodeState::Starting => ("badge-info", "Starting"),
        NodeState::Retiring => ("badge-warning", "Retiring"),
        NodeState::Drained => ("badge-gray", "Drained"),
        NodeState::Draining => ("badge-warning", "Draining"),
        NodeState::Inactive => ("badge-danger", "Inactive"),
        NodeState::Unknown => ("badge-gray", "Unknown"),
    };

    view! {
        <span class=format!("badge {}", class)>{text}</span>
    }
}

/// Badge for node role
#[component]
pub fn RoleBadge(role: NodeRole) -> impl IntoView {
    let (class, text) = match role {
        NodeRole::Meta => ("badge-info", "Meta"),
        NodeRole::Router => ("badge-info", "Router"),
        NodeRole::Data => ("badge-success", "Data"),
        NodeRole::Compactor => ("badge-gray", "Compactor"),
        NodeRole::Unknown => ("badge-gray", "Unknown"),
    };

    view! {
        <span class=format!("badge {}", class)>{text}</span>
    }
}

/// Badge for data node mode
#[component]
pub fn ModeBadge(mode: Option<DataNodeMode>) -> impl IntoView {
    let (class, text) = match mode {
        Some(DataNodeMode::Primary) => ("badge-success", "Primary"),
        Some(DataNodeMode::Standby) => ("badge-warning", "Standby"),
        None => ("badge-gray", "-"),
    };

    view! {
        <span class=format!("badge {}", class)>{text}</span>
    }
}

/// Health indicator dot
#[component]
pub fn HealthDot(healthy: bool) -> impl IntoView {
    view! {
        <span
            class="w-3 h-3 rounded-full inline-block"
            class:bg-green-500=healthy
            class:bg-red-500=!healthy
            title=move || if healthy { "Healthy" } else { "Unhealthy" }
        />
    }
}

/// Cluster health badge
#[component]
pub fn ClusterHealthBadge<F>(healthy: F) -> impl IntoView
where
    F: Fn() -> bool + Clone + 'static,
{
    let healthy_for_class = healthy.clone();
    view! {
        <span class=move || {
            if healthy_for_class() {
                "badge badge-success"
            } else {
                "badge badge-danger"
            }
        }>
            {move || if healthy() { "Healthy" } else { "Degraded" }}
        </span>
    }
}
