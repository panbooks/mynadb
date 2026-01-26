//! Data Node server module
//!
//! Arrow Flight is used for all Data node communication:
//! - DoPut: Write line protocol data
//! - DoGet: Execute SQL queries
//! - DoAction: Admin operations (FLUSH, HEALTH, STATS)

pub mod flight;

pub use flight::{DataNodeFlightService, start_flight_server};
