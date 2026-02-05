//! Dashboard page - cluster overview

use leptos::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::fetch_cluster_status;
use crate::components::{ClusterHealthBadge, Spinner, StatCard};
use crate::state::use_app_state;
use admin_types::ClusterStatus;

/// Dashboard page component
#[component]
pub fn Dashboard() -> impl IntoView {
    let state = use_app_state();
    let (cluster, set_cluster) = create_signal::<Option<ClusterStatus>>(None);
    let (loading, set_loading) = create_signal(true);
    let (error, set_error) = create_signal::<Option<String>>(None);

    // Fetch cluster status on mount
    create_effect(move |_| {
        set_loading.set(true);
        spawn_local(async move {
            match fetch_cluster_status().await {
                Ok(status) => {
                    set_cluster.set(Some(status));
                    set_error.set(None);
                }
                Err(e) => {
                    set_error.set(Some(e.to_string()));
                    state.set_error(format!("Failed to fetch cluster status: {}", e));
                }
            }
            set_loading.set(false);
        });
    });

    // Auto-refresh every 5 seconds
    let refresh_interval = set_interval_with_handle(
        move || {
            spawn_local(async move {
                if let Ok(status) = fetch_cluster_status().await {
                    set_cluster.set(Some(status));
                    set_error.set(None);
                }
            });
        },
        std::time::Duration::from_secs(5),
    );

    // Clean up interval on unmount
    on_cleanup(move || {
        if let Ok(handle) = refresh_interval {
            handle.clear();
        }
    });

    view! {
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <h1 class="text-2xl font-bold text-gray-900">"Cluster Overview"</h1>
                <Show when=move || loading.get()>
                    <Spinner />
                </Show>
            </div>

            <Show
                when=move || error.get().is_some()
                fallback=|| ()
            >
                <div class="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
                    {move || error.get().unwrap_or_default()}
                </div>
            </Show>

            <Show
                when=move || cluster.get().is_some()
                fallback=move || view! {
                    <div class="flex justify-center py-12">
                        <Spinner />
                    </div>
                }
            >
                {move || {
                    let status = cluster.get().unwrap();
                    view! {
                        // Health Status
                        <div class="card">
                            <div class="flex items-center justify-between">
                                <div>
                                    <h2 class="text-lg font-semibold text-gray-900">"Cluster Health"</h2>
                                    <p class="text-sm text-gray-500 mt-1">
                                        "Leader: " {status.meta_leader.clone().unwrap_or_else(|| "No leader".to_string())}
                                        " (term " {status.meta_term} ")"
                                    </p>
                                </div>
                                <ClusterHealthBadge healthy=status.healthy />
                            </div>
                        </div>

                        // Node Statistics
                        <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <StatCard
                                label="Meta Nodes"
                                value=move || format!("{}/{}", status.node_counts.meta.active, status.node_counts.meta.total)
                                variant=Some("info")
                            />
                            <StatCard
                                label="Router Nodes"
                                value=move || format!("{}/{}", status.node_counts.router.active, status.node_counts.router.total)
                                variant=Some("info")
                            />
                            <StatCard
                                label="Data Nodes (Primary)"
                                value=move || format!("{}", status.node_counts.data.primary)
                                variant=Some("success")
                            />
                            <StatCard
                                label="Data Nodes (Standby)"
                                value=move || format!("{}", status.node_counts.data.standby)
                                variant=Some("warning")
                            />
                        </div>

                        // Data Statistics
                        <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <StatCard
                                label="Total Tables"
                                value=move || format!("{}", status.total_tables)
                            />
                            <StatCard
                                label="Total Partitions"
                                value=move || format!("{}", status.total_partitions)
                            />
                            <StatCard
                                label="Retiring Nodes"
                                value=move || format!("{}", status.node_counts.data.retiring)
                                variant=if status.node_counts.data.retiring > 0 { Some("warning") } else { None }
                            />
                            <StatCard
                                label="Drained Nodes"
                                value=move || format!("{}", status.node_counts.data.drained)
                            />
                        </div>

                        // Warnings
                        <Show when=move || !status.warnings.is_empty()>
                            <div class="card bg-yellow-50 border border-yellow-200">
                                <h3 class="font-semibold text-yellow-800 mb-2">"Warnings"</h3>
                                <ul class="list-disc list-inside text-yellow-700 space-y-1">
                                    <For
                                        each=move || status.warnings.clone()
                                        key=|w| w.clone()
                                        children=move |warning| view! { <li>{warning}</li> }
                                    />
                                </ul>
                            </div>
                        </Show>
                    }
                }}
            </Show>
        </div>
    }
}
