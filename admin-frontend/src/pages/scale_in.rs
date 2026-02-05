//! Scale-In page - scale-in wizard

use admin_types::{ScaleInPlan, ScaleInPlanRequest, ScaleInStrategy};
use leptos::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::{create_scale_in_plan, execute_scale_in};
use crate::components::{ProgressBar, Spinner};
use crate::state::use_app_state;

/// Scale-In page component
#[component]
pub fn ScaleIn() -> impl IntoView {
    let state = use_app_state();

    // Wizard state
    let (step, set_step) = create_signal(1);
    let (reduce_num, set_reduce_num) = create_signal(1_usize);
    let (strategy, set_strategy) = create_signal(ScaleInStrategy::LeastLoaded);
    let (plan, set_plan) = create_signal::<Option<ScaleInPlan>>(None);
    let (loading, set_loading) = create_signal(false);
    let (executing, set_executing) = create_signal(false);
    let (error, set_error) = create_signal::<Option<String>>(None);

    // Generate preview plan
    let generate_plan = move |_| {
        set_loading.set(true);
        set_error.set(None);

        let request = ScaleInPlanRequest {
            reduce_num: reduce_num.get(),
            strategy: strategy.get(),
            dry_run: true,
            ..Default::default()
        };

        spawn_local(async move {
            match create_scale_in_plan(&request).await {
                Ok(p) => {
                    set_plan.set(Some(p));
                    set_step.set(2);
                }
                Err(e) => {
                    set_error.set(Some(e.to_string()));
                    state.set_error(format!("Failed to generate plan: {}", e));
                }
            }
            set_loading.set(false);
        });
    };

    // Execute the plan
    let do_execute = move |_| {
        set_executing.set(true);
        set_error.set(None);

        let request = ScaleInPlanRequest {
            reduce_num: reduce_num.get(),
            strategy: strategy.get(),
            dry_run: false,
            force_flush: true,
            ..Default::default()
        };

        spawn_local(async move {
            match execute_scale_in(&request).await {
                Ok(result) => {
                    if result.success {
                        state.add_toast(crate::state::Toast::success(format!(
                            "Scale-in completed: {} nodes retired",
                            result.primaries_retired.len() + result.standbys_retired.len()
                        )));
                        set_step.set(3);
                    } else {
                        let errors = result.errors.join(", ");
                        set_error.set(Some(errors.clone()));
                        state.set_error(format!("Scale-in failed: {}", errors));
                    }
                }
                Err(e) => {
                    set_error.set(Some(e.to_string()));
                    state.set_error(format!("Scale-in execution failed: {}", e));
                }
            }
            set_executing.set(false);
        });
    };

    view! {
        <div class="space-y-6">
            <h1 class="text-2xl font-bold text-gray-900">"Scale-In Operations"</h1>

            // Progress indicator
            <div class="flex items-center justify-center space-x-4">
                <StepIndicator step=1 current=step label="Configure" />
                <div class="w-8 h-0.5 bg-gray-300" />
                <StepIndicator step=2 current=step label="Review" />
                <div class="w-8 h-0.5 bg-gray-300" />
                <StepIndicator step=3 current=step label="Complete" />
            </div>

            // Error display
            <Show when=move || error.get().is_some()>
                <div class="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
                    {move || error.get().unwrap_or_default()}
                </div>
            </Show>

            // Step 1: Configure
            <Show when=move || step.get() == 1>
                <div class="card">
                    <h2 class="card-header">"Configure Scale-In"</h2>

                    <div class="space-y-4">
                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">
                                "Number of nodes to remove"
                            </label>
                            <input
                                type="number"
                                min="1"
                                max="10"
                                class="input w-32"
                                prop:value=move || reduce_num.get()
                                on:input=move |ev| {
                                    if let Ok(v) = event_target_value(&ev).parse() {
                                        set_reduce_num.set(v);
                                    }
                                }
                            />
                        </div>

                        <div>
                            <label class="block text-sm font-medium text-gray-700 mb-1">
                                "Selection Strategy"
                            </label>
                            <select
                                class="input w-64"
                                on:change=move |ev| {
                                    let value = event_target_value(&ev);
                                    let s = match value.as_str() {
                                        "first_in" => ScaleInStrategy::FirstIn,
                                        "last_in" => ScaleInStrategy::LastIn,
                                        "ha_unit" => ScaleInStrategy::HaUnit,
                                        _ => ScaleInStrategy::LeastLoaded,
                                    };
                                    set_strategy.set(s);
                                }
                            >
                                <option value="least_loaded">"Least Loaded (fewest tables)"</option>
                                <option value="first_in">"First In (oldest nodes)"</option>
                                <option value="last_in">"Last In (newest nodes)"</option>
                                <option value="ha_unit">"HA Unit (maintain ratio)"</option>
                            </select>
                        </div>

                        <div class="pt-4">
                            <button
                                class="btn btn-primary"
                                disabled=move || loading.get()
                                on:click=generate_plan
                            >
                                {move || if loading.get() { "Generating..." } else { "Preview Plan" }}
                            </button>
                        </div>
                    </div>
                </div>
            </Show>

            // Step 2: Review Plan
            <Show when=move || step.get() == 2 && plan.get().is_some()>
                {move || {
                    let p = plan.get().unwrap();
                    view! {
                        <div class="card">
                            <h2 class="card-header">"Review Scale-In Plan"</h2>

                            <div class="space-y-4">
                                // Summary
                                <div class="bg-gray-50 rounded-lg p-4">
                                    <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                        <div>
                                            <span class="text-gray-500">"Primaries to retire:"</span>
                                            <span class="font-semibold ml-2">{p.primaries_to_retire.len()}</span>
                                        </div>
                                        <div>
                                            <span class="text-gray-500">"Standbys to retire:"</span>
                                            <span class="font-semibold ml-2">{p.standbys_to_retire.len()}</span>
                                        </div>
                                        <div>
                                            <span class="text-gray-500">"Tables to migrate:"</span>
                                            <span class="font-semibold ml-2">{p.migrations.len()}</span>
                                        </div>
                                        <div>
                                            <span class="text-gray-500">"Phases:"</span>
                                            <span class="font-semibold ml-2">{p.phases.len()}</span>
                                        </div>
                                    </div>
                                </div>

                                // Nodes to retire
                                <Show when=move || !p.primaries_to_retire.is_empty()>
                                    <div>
                                        <h3 class="font-semibold text-gray-700 mb-2">"Primary nodes to retire"</h3>
                                        <div class="flex flex-wrap gap-2">
                                            <For
                                                each=move || p.primaries_to_retire.clone()
                                                key=|n| n.clone()
                                                children=move |node| view! {
                                                    <span class="badge badge-danger">{node}</span>
                                                }
                                            />
                                        </div>
                                    </div>
                                </Show>

                                <Show when=move || !p.standbys_to_retire.is_empty()>
                                    <div>
                                        <h3 class="font-semibold text-gray-700 mb-2">"Standby nodes to retire"</h3>
                                        <div class="flex flex-wrap gap-2">
                                            <For
                                                each=move || p.standbys_to_retire.clone()
                                                key=|n| n.clone()
                                                children=move |node| view! {
                                                    <span class="badge badge-warning">{node}</span>
                                                }
                                            />
                                        </div>
                                    </div>
                                </Show>

                                // Warnings
                                <Show when=move || !p.warnings.is_empty()>
                                    <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                                        <h3 class="font-semibold text-yellow-800 mb-2">"Warnings"</h3>
                                        <ul class="list-disc list-inside text-yellow-700 space-y-1">
                                            <For
                                                each=move || p.warnings.clone()
                                                key=|w| w.clone()
                                                children=move |warning| view! { <li>{warning}</li> }
                                            />
                                        </ul>
                                    </div>
                                </Show>

                                // Final state preview
                                <div class="bg-blue-50 border border-blue-200 rounded-lg p-4">
                                    <h3 class="font-semibold text-blue-800 mb-2">"Final State"</h3>
                                    <div class="text-blue-700 text-sm">
                                        {p.final_state.primary_count} " Primaries, "
                                        {p.final_state.standby_count} " Standbys, "
                                        {p.final_state.total_tables} " Tables"
                                        <Show when=move || p.final_state.ratio_maintained>
                                            <span class="text-green-600 ml-2">"✓ Ratio maintained"</span>
                                        </Show>
                                    </div>
                                </div>

                                // Actions
                                <div class="flex space-x-4 pt-4">
                                    <button
                                        class="btn btn-secondary"
                                        on:click=move |_| set_step.set(1)
                                    >
                                        "Back"
                                    </button>
                                    <button
                                        class="btn btn-danger"
                                        disabled=move || executing.get()
                                        on:click=do_execute
                                    >
                                        {move || if executing.get() { "Executing..." } else { "Execute Scale-In" }}
                                    </button>
                                </div>
                            </div>
                        </div>
                    }
                }}
            </Show>

            // Step 3: Complete
            <Show when=move || step.get() == 3>
                <div class="card text-center py-8">
                    <div class="text-green-500 text-6xl mb-4">"✓"</div>
                    <h2 class="text-xl font-semibold text-gray-900 mb-2">"Scale-In Complete"</h2>
                    <p class="text-gray-600 mb-6">"The scale-in operation has been completed successfully."</p>
                    <button
                        class="btn btn-primary"
                        on:click=move |_| {
                            set_step.set(1);
                            set_plan.set(None);
                        }
                    >
                        "Start New Scale-In"
                    </button>
                </div>
            </Show>
        </div>
    }
}

/// Step indicator component
#[component]
fn StepIndicator(
    step: u8,
    current: ReadSignal<u8>,
    label: &'static str,
) -> impl IntoView {
    let is_active = move || current.get() == step;
    let is_complete = move || current.get() > step;

    view! {
        <div class="flex flex-col items-center">
            <div
                class="w-8 h-8 rounded-full flex items-center justify-center text-sm font-semibold"
                class:bg-myna-primary=is_active
                class:text-white=is_active
                class:bg-green-500=is_complete
                class:text-white=is_complete
                class:bg-gray-200=move || !is_active() && !is_complete()
                class:text-gray-600=move || !is_active() && !is_complete()
            >
                {move || if is_complete() { "✓".to_string() } else { step.to_string() }}
            </div>
            <span class="text-xs text-gray-500 mt-1">{label}</span>
        </div>
    }
}
