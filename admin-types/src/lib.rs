//! Shared types for MynaDB Admin Console
//!
//! This crate provides types shared between the admin backend (Rust/Axum)
//! and the admin frontend (Leptos/WASM).

pub mod cluster;
pub mod events;
pub mod nodes;
pub mod scale_in;
pub mod tables;

// Re-exports for convenience
pub use cluster::*;
pub use events::*;
pub use nodes::*;
pub use scale_in::*;
pub use tables::*;
