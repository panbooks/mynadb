//! Tables page - table management

use admin_types::{ListTablesQuery, TableSummary};
use leptos::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::fetch_tables;
use crate::components::Spinner;
use crate::state::use_app_state;

/// Tables page component
#[component]
pub fn Tables() -> impl IntoView {
    let state = use_app_state();
    let (tables, set_tables) = create_signal::<Vec<TableSummary>>(Vec::new());
    let (total, set_total) = create_signal(0_usize);
    let (loading, set_loading) = create_signal(true);
    let (error, set_error) = create_signal::<Option<String>>(None);
    let (search, set_search) = create_signal(String::new());

    // Trigger signal for manual refresh/search
    let (refresh_trigger, set_refresh_trigger) = create_signal(0_u32);

    // Fetch tables
    let state_clone = state.clone();
    create_effect(move |_| {
        let _ = refresh_trigger.get();
        let state = state_clone.clone();

        set_loading.set(true);
        let query = ListTablesQuery {
            search: {
                let s = search.get();
                if s.is_empty() { None } else { Some(s) }
            },
            limit: 100,
            ..Default::default()
        };

        spawn_local(async move {
            match fetch_tables(&query).await {
                Ok(response) => {
                    set_tables.set(response.tables);
                    set_total.set(response.total);
                    set_error.set(None);
                }
                Err(e) => {
                    set_error.set(Some(e.to_string()));
                    state.set_error(format!("Failed to fetch tables: {}", e));
                }
            }
            set_loading.set(false);
        });
    });

    view! {
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <h1 class="text-2xl font-bold text-gray-900">"Tables"</h1>
                <button class="btn btn-secondary" on:click=move |_| set_refresh_trigger.update(|n| *n += 1)>
                    "Refresh"
                </button>
            </div>

            // Search
            <div class="card">
                <div class="flex gap-4">
                    <input
                        type="text"
                        placeholder="Search tables..."
                        class="input flex-1"
                        prop:value=move || search.get()
                        on:input=move |ev| set_search.set(event_target_value(&ev))
                        on:keydown=move |ev| {
                            if ev.key() == "Enter" {
                                set_refresh_trigger.update(|n| *n += 1);
                            }
                        }
                    />
                    <button class="btn btn-primary" on:click=move |_| set_refresh_trigger.update(|n| *n += 1)>
                        "Search"
                    </button>
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
                    let current_tables = tables.get();
                    if current_tables.is_empty() {
                        view! {
                            <div class="text-center py-12 text-gray-500">
                                "No tables found."
                            </div>
                        }.into_view()
                    } else {
                        view! {
                            <div class="card">
                                <p class="text-sm text-gray-500 mb-4">
                                    "Showing " {current_tables.len()} " of " {total.get()} " tables"
                                </p>

                                <div class="table-container">
                                    <table class="table">
                                        <thead>
                                            <tr>
                                                <th>"Table"</th>
                                                <th>"Primary Owner"</th>
                                                <th>"Partitions"</th>
                                                <th>"Size"</th>
                                                <th>"Rows"</th>
                                            </tr>
                                        </thead>
                                        <tbody class="divide-y divide-gray-200">
                                            <For
                                                each=move || current_tables.clone()
                                                key=|t| format!("{}/{}/{}", t.catalog, t.schema, t.table)
                                                children=move |table| view! { <TableRow table=table /> }
                                            />
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        }.into_view()
                    }
                }}
            </Show>
        </div>
    }
}

/// Table row component
#[component]
fn TableRow(table: TableSummary) -> impl IntoView {
    let _table_key = format!("{}/{}/{}", table.catalog, table.schema, table.table);
    let table_name = table.table.clone();
    let catalog = table.catalog.clone();
    let schema = table.schema.clone();
    let primary_owner = table.primary_owner.clone().unwrap_or_else(|| "-".to_string());
    let owner_count = table.owner_count;
    let partition_count = table.partition_count;
    let total_bytes = table.total_bytes;
    let total_rows = table.total_rows;
    let has_multiple_owners = owner_count > 1;
    let extra_owners = if owner_count > 1 { owner_count - 1 } else { 0 };

    view! {
        <tr>
            <td>
                <div class="font-medium text-gray-900">{table_name}</div>
                <div class="text-xs text-gray-500">{catalog}.{schema}</div>
            </td>
            <td>
                {primary_owner}
                <Show when=move || has_multiple_owners>
                    <span class="text-xs text-gray-400 ml-1">
                        "(+" {extra_owners} ")"
                    </span>
                </Show>
            </td>
            <td>{partition_count}</td>
            <td>{format_bytes(total_bytes)}</td>
            <td>{format_number(total_rows)}</td>
        </tr>
    }
}

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format number with thousand separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
