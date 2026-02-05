//! MynaDB Admin Console Frontend
//!
//! A Leptos-based WASM application for managing MynaDB clusters.

pub mod api;
pub mod app;
pub mod components;
pub mod pages;
pub mod state;

use wasm_bindgen::prelude::*;

/// Initialize the application
#[wasm_bindgen(start)]
pub fn main() {
    // Set up panic hook for better error messages
    console_error_panic_hook::set_once();

    // Mount the app
    leptos::mount_to_body(app::App);
}
