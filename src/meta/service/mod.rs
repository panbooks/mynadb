//! Meta Service gRPC implementations

pub mod grpc;
pub mod meta_node;

pub use grpc::MetaGrpcService;
pub use meta_node::MetaNode;
