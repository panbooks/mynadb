//! Root application component

use leptos::*;
use leptos_router::*;

use crate::components::layout::Layout;
use crate::pages::{dashboard::Dashboard, events::Events, nodes::Nodes, scale_in::ScaleIn, tables::Tables};
use crate::state::provide_app_state;

/// Root application component with routing
#[component]
pub fn App() -> impl IntoView {
    // Provide global application state
    provide_app_state();

    view! {
        <Router>
            <Layout>
                <Routes>
                    <Route path="/" view=Dashboard />
                    <Route path="/nodes" view=Nodes />
                    <Route path="/nodes/:id" view=NodeDetail />
                    <Route path="/scale-in" view=ScaleIn />
                    <Route path="/tables" view=Tables />
                    <Route path="/events" view=Events />
                    <Route path="/*any" view=NotFound />
                </Routes>
            </Layout>
        </Router>
    }
}

/// Node detail page (placeholder - will expand)
#[component]
fn NodeDetail() -> impl IntoView {
    let params = use_params_map();
    let node_id = move || params.with(|p| p.get("id").cloned().unwrap_or_default());

    view! {
        <div class="space-y-6">
            <h1 class="text-2xl font-bold text-gray-900">
                "Node: " {node_id}
            </h1>
            <p class="text-gray-600">"Node details will be displayed here."</p>
        </div>
    }
}

/// 404 Not Found page
#[component]
fn NotFound() -> impl IntoView {
    view! {
        <div class="flex flex-col items-center justify-center min-h-96">
            <h1 class="text-4xl font-bold text-gray-900 mb-4">"404"</h1>
            <p class="text-gray-600 mb-8">"Page not found"</p>
            <A href="/" class="btn btn-primary">"Go to Dashboard"</A>
        </div>
    }
}
