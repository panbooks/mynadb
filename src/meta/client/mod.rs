//! Meta Service client for Router and Data nodes

mod event_subscriber;
mod meta_client;

pub use event_subscriber::{EventSubscriber, EventSubscriberHandle, MetaEvent, SubscriberConfig};
pub use meta_client::MetaClient;
