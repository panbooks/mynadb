//! Data Node server module
//!
//! Arrow Flight is used for all Data node communication:
//! - DoPut: Write line protocol data
//! - DoGet: Execute SQL queries
//! - DoAction: Admin operations (FLUSH, HEALTH, STATS)
//!
//! HTTP is used for basic monitoring endpoints (/health, /stats, /version)

pub mod api;
pub mod flight;

pub use api::{start_http_server, DataNodeState};
pub use flight::{start_flight_server, DataNodeFlightService};
