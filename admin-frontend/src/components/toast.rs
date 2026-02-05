//! Toast notification component

use leptos::*;

use crate::state::{use_app_state, Toast, ToastType};

/// Toast container that displays all active toasts
#[component]
pub fn ToastContainer() -> impl IntoView {
    let state = use_app_state();
    let toasts = state.toasts;

    view! {
        <div class="fixed top-4 right-4 z-50 space-y-2">
            <For
                each=move || toasts.get()
                key=|toast| toast.id
                children=move |toast| view! { <ToastItem toast=toast /> }
            />
        </div>
    }
}

/// Individual toast notification
#[component]
fn ToastItem(toast: Toast) -> impl IntoView {
    let state = use_app_state();
    let state_for_timeout = state.clone();
    let toast_id = toast.id;

    // Auto-dismiss after duration
    set_timeout(
        move || {
            state_for_timeout.remove_toast(toast_id);
        },
        std::time::Duration::from_millis(toast.duration_ms),
    );

    let (bg_class, icon) = match toast.toast_type {
        ToastType::Success => ("bg-green-500", "check"),
        ToastType::Error => ("bg-red-500", "x"),
        ToastType::Warning => ("bg-yellow-500", "warning"),
        ToastType::Info => ("bg-blue-500", "info"),
    };

    view! {
        <div class=format!(
            "{} text-white px-4 py-3 rounded-lg shadow-lg flex items-center space-x-3 min-w-64 max-w-md animate-slide-in",
            bg_class
        )>
            <span class="flex-shrink-0">
                {match icon {
                    "check" => view! { <span class="text-lg">"✓"</span> }.into_view(),
                    "x" => view! { <span class="text-lg">"✕"</span> }.into_view(),
                    "warning" => view! { <span class="text-lg">"⚠"</span> }.into_view(),
                    _ => view! { <span class="text-lg">"ℹ"</span> }.into_view(),
                }}
            </span>
            <span class="flex-1">{toast.message.clone()}</span>
            <button
                class="flex-shrink-0 hover:opacity-75"
                on:click=move |_| state.remove_toast(toast_id)
            >
                "×"
            </button>
        </div>
    }
}
