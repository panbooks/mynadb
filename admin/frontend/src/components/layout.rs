//! Main layout component with sidebar navigation

use leptos::*;
use leptos_router::*;

use crate::state::use_app_state;
use super::toast::ToastContainer;

/// Main layout wrapper
#[component]
pub fn Layout(children: Children) -> impl IntoView {
    view! {
        <div class="min-h-screen flex bg-gray-50/50">
            <Sidebar />
            <main class="flex-1 p-5 overflow-auto">
                <div class="max-w-6xl">
                    {children()}
                </div>
            </main>
            <ToastContainer />
        </div>
    }
}

/// Sidebar navigation
#[component]
fn Sidebar() -> impl IntoView {
    let state = use_app_state();
    let ws_connected = state.ws_connected;

    view! {
        <aside class="w-56 bg-white border-r border-gray-200 flex flex-col">
            // Logo/Header
            <div class="px-4 py-4 border-b border-gray-100">
                <div class="flex items-center gap-2">
                    <div class="w-8 h-8 bg-gradient-to-br from-myna-primary to-blue-600 rounded-lg flex items-center justify-center">
                        <svg class="w-4 h-4 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
                        </svg>
                    </div>
                    <div>
                        <h1 class="text-sm font-semibold text-gray-900">"MynaDB"</h1>
                        <p class="text-xs text-gray-400">"Admin Console"</p>
                    </div>
                </div>
            </div>

            // Navigation links
            <nav class="flex-1 px-3 py-4 space-y-1">
                <NavLink href="/" label="Dashboard" icon="chart-bar" />
                <NavLink href="/nodes" label="Nodes" icon="server" />
                <NavLink href="/scale-in" label="Scale-In" icon="arrow-down-circle" />
                <NavLink href="/tables" label="Tables" icon="table-cells" />
                <NavLink href="/events" label="Events" icon="bell" />
            </nav>

            // Status footer
            <div class="px-4 py-3 border-t border-gray-100">
                <div class="flex items-center gap-2">
                    <span
                        class="w-1.5 h-1.5 rounded-full"
                        class:bg-emerald-500=move || ws_connected.get()
                        class:bg-red-500=move || !ws_connected.get()
                    />
                    <span class="text-xs text-gray-500">
                        {move || if ws_connected.get() { "Connected" } else { "Disconnected" }}
                    </span>
                </div>
            </div>
        </aside>
    }
}

/// Navigation link component
#[component]
fn NavLink(
    href: &'static str,
    label: &'static str,
    #[prop(optional)]
    icon: &'static str,
) -> impl IntoView {
    let location = use_location();
    let is_active = move || {
        let path = location.pathname.get();
        if href == "/" {
            path == "/"
        } else {
            path.starts_with(href)
        }
    };

    view! {
        <A
            href=href
            class=move || if is_active() { "sidebar-link active" } else { "sidebar-link" }
        >
            <Icon name=icon class="w-4 h-4 mr-2.5 flex-shrink-0" />
            <span>{label}</span>
        </A>
    }
}

/// Simple icon component (using heroicons-style names)
#[component]
fn Icon(
    name: &'static str,
    #[prop(optional, default = "")]
    class: &'static str,
) -> impl IntoView {
    // Simple SVG icons - in production, you'd use a proper icon library
    let svg = match name {
        "chart-bar" => r#"<path stroke-linecap="round" stroke-linejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" />"#,
        "server" => r#"<path stroke-linecap="round" stroke-linejoin="round" d="M21.75 17.25v-.228a4.5 4.5 0 0 0-.12-1.03l-2.268-9.64a3.375 3.375 0 0 0-3.285-2.602H7.923a3.375 3.375 0 0 0-3.285 2.602l-2.268 9.64a4.5 4.5 0 0 0-.12 1.03v.228m19.5 0a3 3 0 0 1-3 3H5.25a3 3 0 0 1-3-3m19.5 0a3 3 0 0 0-3-3H5.25a3 3 0 0 0-3 3m16.5 0h.008v.008h-.008v-.008Zm-3 0h.008v.008h-.008v-.008Z" />"#,
        "arrow-down-circle" => r#"<path stroke-linecap="round" stroke-linejoin="round" d="m9 12.75 3 3m0 0 3-3m-3 3v-7.5M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />"#,
        "table-cells" => r#"<path stroke-linecap="round" stroke-linejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 0 1-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0 1 12 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 1.5v-1.5m0 0c0-.621.504-1.125 1.125-1.125m0 0h7.5" />"#,
        "bell" => r#"<path stroke-linecap="round" stroke-linejoin="round" d="M14.857 17.082a23.848 23.848 0 0 0 5.454-1.31A8.967 8.967 0 0 1 18 9.75V9A6 6 0 0 0 6 9v.75a8.967 8.967 0 0 1-2.312 6.022c1.733.64 3.56 1.085 5.455 1.31m5.714 0a24.255 24.255 0 0 1-5.714 0m5.714 0a3 3 0 1 1-5.714 0" />"#,
        _ => r#"<circle cx="12" cy="12" r="10" />"#,
    };

    view! {
        <svg
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
            stroke-width="1.5"
            stroke="currentColor"
            class=class
            inner_html=svg
        />
    }
}
