//! Events page - event viewer

use admin_types::{Event, ListEventsQuery};
use gloo_timers::callback::Interval;
use leptos::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::fetch_events;
use crate::components::Spinner;
use crate::state::use_app_state;

/// Events page component
#[component]
pub fn Events() -> impl IntoView {
    let state = use_app_state();
    let (events, set_events) = create_signal::<Vec<Event>>(Vec::new());
    let (loading, set_loading) = create_signal(true);
    let (error, set_error) = create_signal::<Option<String>>(None);
    let (event_type_filter, set_event_type_filter) = create_signal::<Option<String>>(None);

    // Trigger signal for manual refresh
    let (refresh_trigger, set_refresh_trigger) = create_signal(0_u32);

    // Fetch events - runs on filter change or refresh trigger
    let state_clone = state.clone();
    create_effect(move |_| {
        let event_type = event_type_filter.get();
        let _ = refresh_trigger.get();
        let state = state_clone.clone();

        set_loading.set(true);
        let query = ListEventsQuery {
            event_type,
            limit: 100,
            ..Default::default()
        };

        spawn_local(async move {
            match fetch_events(&query).await {
                Ok(response) => {
                    set_events.set(response.events);
                    set_error.set(None);
                }
                Err(e) => {
                    set_error.set(Some(e.to_string()));
                    state.set_error(format!("Failed to fetch events: {}", e));
                }
            }
            set_loading.set(false);
        });
    });

    // Auto-refresh every 10 seconds
    let interval = Interval::new(10000, move || {
        set_refresh_trigger.update(|n| *n += 1);
    });

    on_cleanup(move || {
        drop(interval);
    });

    view! {
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <h1 class="text-2xl font-bold text-gray-900">"Events"</h1>
                <button class="btn btn-secondary" on:click=move |_| set_refresh_trigger.update(|n| *n += 1)>
                    "Refresh"
                </button>
            </div>

            // Filter
            <div class="card">
                <div class="flex gap-4">
                    <div>
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Event Type"</label>
                        <select
                            class="input w-48"
                            on:change=move |ev| {
                                let value = event_target_value(&ev);
                                set_event_type_filter.set(if value.is_empty() { None } else { Some(value) });
                            }
                        >
                            <option value="">"All Events"</option>
                            <option value="node_joined">"Node Joined"</option>
                            <option value="node_left">"Node Left"</option>
                            <option value="node_state_changed">"Node State Changed"</option>
                            <option value="leader_changed">"Leader Changed"</option>
                            <option value="table_reassigned">"Table Reassigned"</option>
                            <option value="table_owner_added">"Table Owner Added"</option>
                            <option value="table_owner_removed">"Table Owner Removed"</option>
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
                    let current_events = events.get();
                    if current_events.is_empty() {
                        view! {
                            <div class="text-center py-12 text-gray-500">
                                "No events found."
                            </div>
                        }.into_view()
                    } else {
                        view! {
                            <div class="card">
                                <p class="text-sm text-gray-500 mb-4">
                                    {current_events.len()} " events"
                                </p>

                                <div class="space-y-2">
                                    <For
                                        each=move || current_events.clone()
                                        key=|e| e.sequence
                                        children=move |event| view! { <EventRow event=event /> }
                                    />
                                </div>
                            </div>
                        }.into_view()
                    }
                }}
            </Show>
        </div>
    }
}

/// Event row component
#[component]
fn EventRow(event: Event) -> impl IntoView {
    let event_type_str = format!("{}", event.event_type);
    let timestamp = format_timestamp(event.timestamp);
    let payload_str = serde_json::to_string_pretty(&event.payload).unwrap_or_default();

    let (expanded, set_expanded) = create_signal(false);

    view! {
        <div class="border rounded-lg p-3 hover:bg-gray-50">
            <div
                class="flex items-center justify-between cursor-pointer"
                on:click=move |_| set_expanded.update(|e| *e = !*e)
            >
                <div class="flex items-center space-x-4">
                    <EventTypeBadge event_type=event.event_type />
                    <span class="text-sm text-gray-600">{event.topic.clone()}</span>
                </div>
                <div class="flex items-center space-x-4 text-sm text-gray-500">
                    <span>"#{" {event.sequence} "}"</span>
                    <span>{timestamp}</span>
                    <span class="text-xs">{move || if expanded.get() { "▼" } else { "▶" }}</span>
                </div>
            </div>

            <Show when=move || expanded.get()>
                <div class="mt-3 pt-3 border-t">
                    <div class="text-xs text-gray-500 mb-2">
                        "Source: " {event.source_node.clone()}
                    </div>
                    <pre class="bg-gray-100 rounded p-2 text-xs overflow-x-auto">
                        {payload_str.clone()}
                    </pre>
                </div>
            </Show>
        </div>
    }
}

/// Event type badge
#[component]
fn EventTypeBadge(event_type: admin_types::EventType) -> impl IntoView {
    use admin_types::EventType;

    let (class, text) = match event_type {
        EventType::NodeJoined => ("badge-success", "Node Joined"),
        EventType::NodeLeft => ("badge-danger", "Node Left"),
        EventType::NodeStateChanged => ("badge-warning", "State Changed"),
        EventType::LeaderChanged => ("badge-info", "Leader Changed"),
        EventType::TableReassigned => ("badge-info", "Table Reassigned"),
        EventType::TableOwnerAdded => ("badge-success", "Owner Added"),
        EventType::TableOwnerRemoved => ("badge-warning", "Owner Removed"),
        EventType::CacheInvalidate => ("badge-gray", "Cache Invalidate"),
        EventType::SchemaChanged => ("badge-info", "Schema Changed"),
        EventType::FlushForRecovery => ("badge-danger", "Flush Recovery"),
        EventType::DistributionConfigChanged => ("badge-info", "Distribution Changed"),
        EventType::TablesCacheRefresh => ("badge-gray", "Cache Refresh"),
        EventType::Unknown => ("badge-gray", "Unknown"),
    };

    view! {
        <span class=format!("badge {}", class)>{text}</span>
    }
}

/// Format timestamp
fn format_timestamp(ts: i64) -> String {
    use chrono::{TimeZone, Utc};
    if let Some(dt) = Utc.timestamp_millis_opt(ts).single() {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        "Invalid".to_string()
    }
}
