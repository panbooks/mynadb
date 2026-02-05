//! Progress bar and loading components

use leptos::*;

/// Progress bar component
#[component]
pub fn ProgressBar(
    /// Progress percentage (0-100)
    progress: u8,
    #[prop(optional)]
    class: &'static str,
) -> impl IntoView {
    let width = format!("{}%", progress.min(100));

    view! {
        <div class=format!("progress-bar {}", class)>
            <div class="progress-bar-fill" style:width=width />
        </div>
    }
}

/// Loading spinner
#[component]
pub fn Spinner(
    #[prop(optional, default = "")]
    class: &'static str,
) -> impl IntoView {
    view! {
        <div class=format!("spinner {}", class) />
    }
}

/// Loading overlay
#[component]
pub fn LoadingOverlay(
    #[prop(optional)]
    message: Option<&'static str>,
) -> impl IntoView {
    view! {
        <div class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div class="bg-white rounded-lg p-6 flex flex-col items-center">
                <Spinner class="w-8 h-8" />
                <Show when=move || message.is_some()>
                    <p class="mt-4 text-gray-600">{message.unwrap_or_default()}</p>
                </Show>
            </div>
        </div>
    }
}

/// Stats card component
#[component]
pub fn StatCard(
    label: &'static str,
    value: impl Fn() -> String + 'static,
    #[prop(optional)]
    subtext: Option<String>,
    #[prop(optional)]
    variant: Option<&'static str>,
) -> impl IntoView {
    let border_class = match variant {
        Some("success") => "border-l-4 border-green-500",
        Some("warning") => "border-l-4 border-yellow-500",
        Some("danger") => "border-l-4 border-red-500",
        Some("info") => "border-l-4 border-blue-500",
        _ => "",
    };

    view! {
        <div class=format!("stat-card {}", border_class)>
            <div class="stat-value">{value}</div>
            <div class="stat-label">{label}</div>
            <Show when=move || subtext.is_some()>
                <div class="text-xs text-gray-400 mt-1">{subtext.clone().unwrap_or_default()}</div>
            </Show>
        </div>
    }
}

/// Empty state component
#[component]
pub fn EmptyState(
    message: &'static str,
    #[prop(optional)]
    action_label: Option<&'static str>,
    #[prop(optional)]
    on_action: Option<Box<dyn Fn()>>,
) -> impl IntoView {
    view! {
        <div class="text-center py-12">
            <div class="text-gray-400 text-6xl mb-4">"()"</div>
            <p class="text-gray-600">{message}</p>
            <Show when=move || action_label.is_some()>
                <button
                    class="btn btn-primary mt-4"
                    on:click=move |_| {
                        if let Some(ref action) = on_action {
                            action();
                        }
                    }
                >
                    {action_label.unwrap_or_default()}
                </button>
            </Show>
        </div>
    }
}

/// Error state component
#[component]
pub fn ErrorState(
    message: String,
    #[prop(optional)]
    on_retry: Option<Box<dyn Fn()>>,
) -> impl IntoView {
    view! {
        <div class="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
            <div class="text-red-500 text-4xl mb-4">"!"</div>
            <p class="text-red-700">{message}</p>
            <Show when=move || on_retry.is_some()>
                <button
                    class="btn btn-danger mt-4"
                    on:click=move |_| {
                        if let Some(ref retry) = on_retry {
                            retry();
                        }
                    }
                >
                    "Retry"
                </button>
            </Show>
        </div>
    }
}
