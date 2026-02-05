//! Node runners for different MynaDB modes
//!
//! This module contains the entry points for running MynaDB in different modes:
//! - `standalone` - All-in-one mode (default)
//! - `meta` - Raft-based metadata service node
//! - `router` - Client protocol handler and request router
//! - `data` - Storage and query execution node
//! - `admin` - Admin console for cluster management

pub mod admin;
pub mod common;
pub mod data;
pub mod meta;
pub mod router;
pub mod standalone;
