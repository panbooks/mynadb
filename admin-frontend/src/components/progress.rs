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
    let indicator_class = match variant {
        Some("success") => "bg-emerald-500",
        Some("warning") => "bg-amber-500",
        Some("danger") => "bg-red-500",
        Some("info") => "bg-blue-500",
        _ => "bg-gray-300",
    };

    let has_subtext = subtext.is_some();
    let subtext_value = subtext.clone().unwrap_or_default();

    view! {
        <div class="stat-card">
            <div class="flex items-center gap-2 mb-1">
                <div class=format!("w-1.5 h-1.5 rounded-full {}", indicator_class) />
                <div class="stat-label">{label}</div>
            </div>
            <div class="stat-value">{value}</div>
            <Show when=move || has_subtext>
                <div class="text-xs text-gray-400 mt-1">{subtext_value.clone()}</div>
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
) -> impl IntoView {
    view! {
        <div class="text-center py-12">
            <div class="text-gray-400 text-6xl mb-4">"()"</div>
            <p class="text-gray-600">{message}</p>
            {action_label.map(|label| view! {
                <button class="btn btn-primary mt-4">
                    {label}
                </button>
            })}
        </div>
    }
}

/// Error state component
#[component]
pub fn ErrorState(
    message: String,
) -> impl IntoView {
    view! {
        <div class="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
            <div class="text-red-500 text-4xl mb-4">"!"</div>
            <p class="text-red-700">{message}</p>
        </div>
    }
}
