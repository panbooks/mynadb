//! State Machine Operations
//!
//! This module contains the implementation of MetaStateMachine operations,
//! split into logical groups for maintainability.

mod election;
mod event;
mod file;
mod lock;
mod node;
mod query;
mod recovery;
mod replica;
mod scale_in;
mod standby;

// All modules implement methods on MetaStateMachine via `impl` blocks,
// so no re-exports are needed - the methods are available on the type directly.
