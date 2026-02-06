//! Node card component for displaying node information

use admin_types::NodeSummary;
use leptos::*;
use leptos_router::*;

use super::{ModeBadge, RoleBadge, StateBadge};

/// Card displaying node summary information
#[component]
pub fn NodeCard(node: NodeSummary) -> impl IntoView {
    let node_id = node.node_id.clone();
    let advertise_addr = node.advertise_addr.clone();
    let role = node.role;
    let state = node.state;
    let mode = node.mode.clone();
    let mode_for_show = node.mode.clone();
    let zone = node.zone.clone();
    let zone_for_show = node.zone.clone();
    let tables_owned = node.tables_owned;
    let last_heartbeat_ago_secs = node.last_heartbeat_ago_secs;

    view! {
        <A
            href=format!("/nodes/{}", node_id)
            class="card hover:shadow-lg transition-shadow cursor-pointer"
        >
            <div class="flex items-start justify-between">
                <div>
                    <h3 class="font-semibold text-gray-900">{node_id}</h3>
                    <p class="text-sm text-gray-500 mt-1">{advertise_addr}</p>
                </div>
                <div class="flex flex-col items-end space-y-1">
                    <RoleBadge role=role />
                    <StateBadge state=state />
                </div>
            </div>

            <div class="mt-4 flex items-center justify-between text-sm">
                <div class="flex items-center space-x-4">
                    <Show when=move || mode_for_show.is_some()>
                        <ModeBadge mode=mode />
                    </Show>
                    <Show when=move || zone_for_show.is_some()>
                        <span class="text-gray-500">
                            "Zone: " {zone.clone().unwrap_or_default()}
                        </span>
                    </Show>
                </div>
                <div class="text-gray-600">
                    {tables_owned} " tables"
                </div>
            </div>

            <div class="mt-2 text-xs text-gray-400">
                "Last heartbeat: " {format_heartbeat_ago(last_heartbeat_ago_secs)}
            </div>
        </A>
    }
}

/// Format heartbeat ago time
fn format_heartbeat_ago(secs: i64) -> String {
    if secs < 0 {
        return "unknown".to_string();
    }
    if secs < 60 {
        format!("{}s ago", secs)
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else {
        format!("{}h ago", secs / 3600)
    }
}

/// Node list component
#[component]
pub fn NodeList(nodes: Vec<NodeSummary>) -> impl IntoView {
    view! {
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <For
                each=move || nodes.clone()
                key=|node| node.node_id.clone()
                children=move |node| view! { <NodeCard node=node /> }
            />
        </div>
    }
}
