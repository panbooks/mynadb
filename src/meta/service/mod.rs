//! Meta Service gRPC and HTTP implementations

pub mod grpc;
pub mod http;
pub mod meta_node;

pub use grpc::MetaGrpcService;
pub use http::{start_http_server, MetaHttpState};
pub use meta_node::MetaNode;
