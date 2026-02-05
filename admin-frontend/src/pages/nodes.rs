//! Nodes page - node management

use admin_types::{ListNodesQuery, NodeSummary};
use leptos::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::fetch_nodes;
use crate::components::{NodeList, Spinner};
use crate::state::use_app_state;

/// Nodes page component
#[component]
pub fn Nodes() -> impl IntoView {
    let state = use_app_state();
    let (nodes, set_nodes) = create_signal::<Vec<NodeSummary>>(Vec::new());
    let (loading, set_loading) = create_signal(true);
    let (error, set_error) = create_signal::<Option<String>>(None);

    // Filter state
    let (role_filter, set_role_filter) = create_signal::<Option<String>>(None);
    let (state_filter, set_state_filter) = create_signal::<Option<String>>(None);
    let (mode_filter, set_mode_filter) = create_signal::<Option<String>>(None);

    // Trigger signal for manual refresh
    let (refresh_trigger, set_refresh_trigger) = create_signal(0_u32);

    // Fetch nodes - this effect runs on filter changes or refresh trigger
    let state_clone = state.clone();
    create_effect(move |_| {
        // Subscribe to filter signals and refresh trigger
        let role = role_filter.get();
        let state_val = state_filter.get();
        let mode = mode_filter.get();
        let _ = refresh_trigger.get();

        let state = state_clone.clone();
        set_loading.set(true);
        let query = ListNodesQuery {
            role,
            state: state_val,
            mode,
        };

        spawn_local(async move {
            match fetch_nodes(&query).await {
                Ok(response) => {
                    set_nodes.set(response.nodes);
                    set_error.set(None);
                }
                Err(e) => {
                    set_error.set(Some(e.to_string()));
                    state.set_error(format!("Failed to fetch nodes: {}", e));
                }
            }
            set_loading.set(false);
        });
    });

    view! {
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <h1 class="text-2xl font-bold text-gray-900">"Nodes"</h1>
                <button class="btn btn-secondary" on:click=move |_| set_refresh_trigger.update(|n| *n += 1)>
                    "Refresh"
                </button>
            </div>

            // Filters
            <div class="card">
                <div class="flex flex-wrap gap-4">
                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Role"</label>
                        <select
                            class="input w-40"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                set_role_filter.set(if value.is_empty() { None } else { Some(value) });
                            }
                        >
                            <option value="">"All"</option>
                            <option value="meta">"Meta"</option>
                            <option value="router">"Router"</option>
                            <option value="data">"Data"</option>
                            <option value="compactor">"Compactor"</option>
                        </select>
                    </div>

                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">"State"</label>
                        <select
                            class="input w-40"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                set_state_filter.set(if value.is_empty() { None } else { Some(value) });
                            }
                        >
                            <option value="">"All"</option>
                            <option value="active">"Active"</option>
                            <option value="starting">"Starting"</option>
                            <option value="retiring">"Retiring"</option>
                            <option value="drained">"Drained"</option>
                            <option value="inactive">"Inactive"</option>
                        </select>
                    </div>

                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Mode"</label>
                        <select
                            class="input w-40"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                set_mode_filter.set(if value.is_empty() { None } else { Some(value) });
                            }
                        >
                            <option value="">"All"</option>
                            <option value="primary">"Primary"</option>
                            <option value="standby">"Standby"</option>
                        </select>
                    </div>
                </div>
            </div>

            // Error display
            <Show when=move || error.get().is_some()>
                <div class="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
                    {move || error.get().unwrap_or_default()}
                </div>
            </Show>

            // Loading or content
            <Show
                when=move || !loading.get()
                fallback=move || view! {
                    <div class="flex justify-center py-12">
                        <Spinner />
                    </div>
                }
            >
                {move || {
                    let current_nodes = nodes.get();
                    if current_nodes.is_empty() {
                        view! {
                            <div class="text-center py-12 text-gray-500">
                                "No nodes found matching the filters."
                            </div>
                        }.into_view()
                    } else {
                        view! {
                            <div>
                                <p class="text-sm text-gray-500 mb-4">
                                    {current_nodes.len()} " nodes"
                                </p>
                                <NodeList nodes=current_nodes />
                            </div>
                        }.into_view()
                    }
                }}
            </Show>
        </div>
    }
}
