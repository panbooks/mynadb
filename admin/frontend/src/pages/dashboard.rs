//! Dashboard page - cluster overview

use leptos::*;
use leptos_router::*;
use gloo_timers::callback::Interval;
use wasm_bindgen_futures::spawn_local;

use crate::api::fetch_cluster_status;
use crate::components::Spinner;
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
    let state_clone = state.clone();
    create_effect(move |_| {
        let state = state_clone.clone();
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
    let interval = Interval::new(5000, move || {
        spawn_local(async move {
            if let Ok(status) = fetch_cluster_status().await {
                set_cluster.set(Some(status));
                set_error.set(None);
            }
        });
    });

    // Clean up interval on unmount
    on_cleanup(move || {
        drop(interval);
    });

    view! {
        <div class="space-y-6">
            <Show
                when=move || error.get().is_some()
                fallback=|| ()
            >
                <div class="bg-red-50 border border-red-200 rounded-xl p-4 text-sm text-red-700 flex items-center gap-3">
                    <svg class="w-5 h-5 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
                    </svg>
                    {move || error.get().unwrap_or_default()}
                </div>
            </Show>

            <Show
                when=move || cluster.get().is_some()
                fallback=move || view! {
                    <div class="flex flex-col items-center justify-center py-24">
                        <Spinner />
                        <p class="mt-4 text-sm text-gray-500">"Loading cluster status..."</p>
                    </div>
                }
            >
                <ClusterDashboardContent cluster=cluster loading=loading />
            </Show>
        </div>
    }
}

/// Dashboard content showing cluster status
#[component]
fn ClusterDashboardContent(
    cluster: ReadSignal<Option<ClusterStatus>>,
    loading: ReadSignal<bool>,
) -> impl IntoView {
    // Create derived signals for the values
    let meta_active = move || cluster.get().map(|s| s.node_counts.meta.active).unwrap_or(0);
    let meta_total = move || cluster.get().map(|s| s.node_counts.meta.total).unwrap_or(0);
    let router_active = move || cluster.get().map(|s| s.node_counts.router.active).unwrap_or(0);
    let router_total = move || cluster.get().map(|s| s.node_counts.router.total).unwrap_or(0);
    let data_active = move || cluster.get().map(|s| s.node_counts.data.active).unwrap_or(0);
    let data_total = move || cluster.get().map(|s| s.node_counts.data.total).unwrap_or(0);
    let primary_count = move || cluster.get().map(|s| s.node_counts.data.primary).unwrap_or(0);
    let standby_count = move || cluster.get().map(|s| s.node_counts.data.standby).unwrap_or(0);
    let retiring_count = move || cluster.get().map(|s| s.node_counts.data.retiring).unwrap_or(0);
    let total_tables = move || cluster.get().map(|s| s.total_tables).unwrap_or(0);
    let total_partitions = move || cluster.get().map(|s| s.total_partitions).unwrap_or(0);
    let healthy = move || cluster.get().map(|s| s.healthy).unwrap_or(false);
    let meta_leader = move || cluster.get().and_then(|s| s.meta_leader.clone()).unwrap_or_else(|| "-".to_string());
    let meta_term = move || cluster.get().map(|s| s.meta_term).unwrap_or(0);
    let warnings = move || cluster.get().map(|s| s.warnings.clone()).unwrap_or_default();
    let has_warnings = move || !warnings().is_empty();
    let total_nodes = move || data_active() + meta_active() + router_active();

    view! {
        // Hero Section - Cluster Health Overview
        <div class="relative overflow-hidden rounded-2xl bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6 text-white">
            // Background pattern
            <div class="absolute inset-0 opacity-10">
                <svg class="h-full w-full" viewBox="0 0 100 100" preserveAspectRatio="none">
                    <defs>
                        <pattern id="grid" width="10" height="10" patternUnits="userSpaceOnUse">
                            <path d="M 10 0 L 0 0 0 10" fill="none" stroke="white" stroke-width="0.5"/>
                        </pattern>
                    </defs>
                    <rect width="100" height="100" fill="url(#grid)" />
                </svg>
            </div>

            <div class="relative flex items-start justify-between">
                <div class="flex-1">
                    <div class="flex items-center gap-3 mb-4">
                        <div class=move || {
                            if healthy() {
                                "w-3 h-3 rounded-full bg-emerald-400 shadow-lg shadow-emerald-400/50 animate-pulse"
                            } else {
                                "w-3 h-3 rounded-full bg-red-400 shadow-lg shadow-red-400/50 animate-pulse"
                            }
                        } />
                        <span class="text-sm font-medium text-slate-300">
                            {move || if healthy() { "All Systems Operational" } else { "Issues Detected" }}
                        </span>
                        <Show when=move || loading.get()>
                            <div class="ml-2">
                                <Spinner />
                            </div>
                        </Show>
                    </div>

                    <h1 class="text-3xl font-bold tracking-tight mb-1">"MynaDB Cluster"</h1>
                    <p class="text-slate-400 text-sm">
                        "Leader: " <span class="text-white font-mono">{meta_leader}</span>
                        " · Term " <span class="text-white">{meta_term}</span>
                    </p>
                </div>

                // Quick Stats
                <div class="hidden sm:flex items-center gap-8">
                    <div class="text-center">
                        <div class="text-3xl font-bold">{total_nodes}</div>
                        <div class="text-xs text-slate-400 uppercase tracking-wide">"Active Nodes"</div>
                    </div>
                    <div class="text-center">
                        <div class="text-3xl font-bold">{total_tables}</div>
                        <div class="text-xs text-slate-400 uppercase tracking-wide">"Tables"</div>
                    </div>
                    <div class="text-center">
                        <div class="text-3xl font-bold">{total_partitions}</div>
                        <div class="text-xs text-slate-400 uppercase tracking-wide">"Partitions"</div>
                    </div>
                </div>
            </div>
        </div>

        // Warnings Banner
        <Show when=has_warnings>
            <div class="rounded-xl bg-gradient-to-r from-amber-50 to-orange-50 border border-amber-200 p-4">
                <div class="flex items-start gap-3">
                    <div class="w-8 h-8 rounded-lg bg-amber-100 flex items-center justify-center flex-shrink-0">
                        <svg class="w-4 h-4 text-amber-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                        </svg>
                    </div>
                    <div class="flex-1">
                        <h3 class="text-sm font-semibold text-amber-900">{move || format!("{} Warning(s)", warnings().len())}</h3>
                        <ul class="mt-1 text-sm text-amber-700 space-y-0.5">
                            <For
                                each=warnings
                                key=|w| w.clone()
                                children=move |warning| view! { <li>{warning}</li> }
                            />
                        </ul>
                    </div>
                </div>
            </div>
        </Show>

        // Main Grid - Node Overview
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            // Data Nodes Card
            <div class="col-span-1 md:col-span-2 lg:col-span-1 bg-white rounded-xl border border-gray-200 p-5 hover:shadow-lg transition-shadow">
                <div class="flex items-center justify-between mb-4">
                    <div class="flex items-center gap-3">
                        <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-blue-600 flex items-center justify-center shadow-lg shadow-blue-500/25">
                            <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M5.25 14.25h13.5m-13.5 0a3 3 0 01-3-3m3 3a3 3 0 100 6h13.5a3 3 0 100-6m-16.5-3a3 3 0 013-3h13.5a3 3 0 013 3m-19.5 0a4.5 4.5 0 01.9-2.7L5.737 5.1a3.375 3.375 0 012.7-1.35h7.126c1.062 0 2.062.5 2.7 1.35l2.587 3.45a4.5 4.5 0 01.9 2.7m0 0a3 3 0 01-3 3m0 3h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008zm-3 6h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008z" />
                            </svg>
                        </div>
                        <div>
                            <h3 class="font-semibold text-gray-900">"Data Nodes"</h3>
                            <p class="text-xs text-gray-500">"Storage & query processing"</p>
                        </div>
                    </div>
                    <A href="/nodes" class="text-xs text-blue-600 hover:text-blue-700 font-medium">"View All →"</A>
                </div>

                <div class="grid grid-cols-2 gap-4">
                    <div class="bg-emerald-50 rounded-lg p-3">
                        <div class="flex items-center gap-2 mb-1">
                            <div class="w-2 h-2 rounded-full bg-emerald-500"></div>
                            <span class="text-xs font-medium text-emerald-700">"Primary"</span>
                        </div>
                        <div class="text-2xl font-bold text-emerald-900">{primary_count}</div>
                    </div>
                    <div class="bg-amber-50 rounded-lg p-3">
                        <div class="flex items-center gap-2 mb-1">
                            <div class="w-2 h-2 rounded-full bg-amber-500"></div>
                            <span class="text-xs font-medium text-amber-700">"Standby"</span>
                        </div>
                        <div class="text-2xl font-bold text-amber-900">{standby_count}</div>
                    </div>
                </div>

                {move || {
                    if retiring_count() > 0 {
                        Some(view! {
                            <div class="mt-3 pt-3 border-t border-gray-100">
                                <div class="flex items-center justify-between text-sm">
                                    <span class="text-gray-500">"Retiring"</span>
                                    <span class="font-medium text-orange-600">{retiring_count}</span>
                                </div>
                            </div>
                        })
                    } else {
                        None
                    }
                }}
            </div>

            // Meta Nodes Card
            <div class="bg-white rounded-xl border border-gray-200 p-5 hover:shadow-lg transition-shadow">
                <div class="flex items-center gap-3 mb-4">
                    <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-500 to-purple-600 flex items-center justify-center shadow-lg shadow-violet-500/25">
                        <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
                        </svg>
                    </div>
                    <div>
                        <h3 class="font-semibold text-gray-900">"Meta Nodes"</h3>
                        <p class="text-xs text-gray-500">"Cluster coordination"</p>
                    </div>
                </div>

                <div class="flex items-end justify-between">
                    <div>
                        <div class="text-3xl font-bold text-gray-900">
                            {meta_active}
                            <span class="text-lg text-gray-400 font-normal">" / "{meta_total}</span>
                        </div>
                        <p class="text-xs text-gray-500 mt-1">"Active nodes"</p>
                    </div>
                    <div class=move || {
                        if meta_active() == meta_total() && meta_total() > 0 {
                            "px-2 py-1 rounded-full bg-emerald-100 text-emerald-700 text-xs font-medium"
                        } else if meta_active() > 0 {
                            "px-2 py-1 rounded-full bg-amber-100 text-amber-700 text-xs font-medium"
                        } else {
                            "px-2 py-1 rounded-full bg-gray-100 text-gray-500 text-xs font-medium"
                        }
                    }>
                        {move || {
                            if meta_total() == 0 { "No nodes" }
                            else if meta_active() == meta_total() { "Healthy" }
                            else { "Degraded" }
                        }}
                    </div>
                </div>
            </div>

            // Router Nodes Card
            <div class="bg-white rounded-xl border border-gray-200 p-5 hover:shadow-lg transition-shadow">
                <div class="flex items-center gap-3 mb-4">
                    <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-cyan-500 to-teal-600 flex items-center justify-center shadow-lg shadow-cyan-500/25">
                        <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
                        </svg>
                    </div>
                    <div>
                        <h3 class="font-semibold text-gray-900">"Router Nodes"</h3>
                        <p class="text-xs text-gray-500">"Query routing"</p>
                    </div>
                </div>

                <div class="flex items-end justify-between">
                    <div>
                        <div class="text-3xl font-bold text-gray-900">
                            {router_active}
                            <span class="text-lg text-gray-400 font-normal">" / "{router_total}</span>
                        </div>
                        <p class="text-xs text-gray-500 mt-1">"Active nodes"</p>
                    </div>
                    <div class=move || {
                        if router_active() == router_total() && router_total() > 0 {
                            "px-2 py-1 rounded-full bg-emerald-100 text-emerald-700 text-xs font-medium"
                        } else if router_active() > 0 {
                            "px-2 py-1 rounded-full bg-amber-100 text-amber-700 text-xs font-medium"
                        } else {
                            "px-2 py-1 rounded-full bg-gray-100 text-gray-500 text-xs font-medium"
                        }
                    }>
                        {move || {
                            if router_total() == 0 { "No nodes" }
                            else if router_active() == router_total() { "Healthy" }
                            else { "Degraded" }
                        }}
                    </div>
                </div>
            </div>
        </div>

        // Quick Actions
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <A href="/nodes" class="group flex items-center gap-4 p-4 bg-white rounded-xl border border-gray-200 hover:border-blue-300 hover:shadow-md transition-all">
                <div class="w-12 h-12 rounded-xl bg-blue-50 group-hover:bg-blue-100 flex items-center justify-center transition-colors">
                    <svg class="w-6 h-6 text-blue-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M5.25 14.25h13.5m-13.5 0a3 3 0 01-3-3m3 3a3 3 0 100 6h13.5a3 3 0 100-6m-16.5-3a3 3 0 013-3h13.5a3 3 0 013 3m-19.5 0a4.5 4.5 0 01.9-2.7L5.737 5.1a3.375 3.375 0 012.7-1.35h7.126c1.062 0 2.062.5 2.7 1.35l2.587 3.45a4.5 4.5 0 01.9 2.7m0 0a3 3 0 01-3 3m0 3h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008zm-3 6h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008z" />
                    </svg>
                </div>
                <div>
                    <h3 class="font-medium text-gray-900 group-hover:text-blue-600 transition-colors">"Manage Nodes"</h3>
                    <p class="text-xs text-gray-500">"View and manage cluster nodes"</p>
                </div>
            </A>

            <A href="/tables" class="group flex items-center gap-4 p-4 bg-white rounded-xl border border-gray-200 hover:border-emerald-300 hover:shadow-md transition-all">
                <div class="w-12 h-12 rounded-xl bg-emerald-50 group-hover:bg-emerald-100 flex items-center justify-center transition-colors">
                    <svg class="w-6 h-6 text-emerald-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 1.5v-1.5m0 0c0-.621.504-1.125 1.125-1.125m0 0h7.5" />
                    </svg>
                </div>
                <div>
                    <h3 class="font-medium text-gray-900 group-hover:text-emerald-600 transition-colors">"Browse Tables"</h3>
                    <p class="text-xs text-gray-500">"View table metadata and partitions"</p>
                </div>
            </A>

            <A href="/events" class="group flex items-center gap-4 p-4 bg-white rounded-xl border border-gray-200 hover:border-violet-300 hover:shadow-md transition-all">
                <div class="w-12 h-12 rounded-xl bg-violet-50 group-hover:bg-violet-100 flex items-center justify-center transition-colors">
                    <svg class="w-6 h-6 text-violet-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M14.857 17.082a23.848 23.848 0 005.454-1.31A8.967 8.967 0 0118 9.75v-.75a6 6 0 00-6-6 6 6 0 00-6 6v.75a8.967 8.967 0 01-2.312 6.022c1.733.64 3.56 1.085 5.455 1.31m5.714 0a24.255 24.255 0 01-5.714 0m5.714 0a3 3 0 11-5.714 0" />
                    </svg>
                </div>
                <div>
                    <h3 class="font-medium text-gray-900 group-hover:text-violet-600 transition-colors">"View Events"</h3>
                    <p class="text-xs text-gray-500">"Monitor cluster activity"</p>
                </div>
            </A>
        </div>

        // Footer with auto-refresh indicator
        <div class="flex items-center justify-center gap-2 text-xs text-gray-400 pt-4">
            <div class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></div>
            <span>"Auto-refreshing every 5 seconds"</span>
        </div>
    }
}
