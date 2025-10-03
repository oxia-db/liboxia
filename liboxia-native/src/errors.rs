use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum OxiaError {
    #[error("unexpected transport error: {0}")]
    Transport(String),

    #[error("unexpected grpc status: {0}")]
    GrpcStatus(#[from] tonic::Status),

    #[error("unexpected status: {0}")]
    UnexpectedStatus(String),

    #[error("shard leader not found.  shard={0}")]
    ShardLeaderNotFound(i64),

    #[error("key leader not found.  key={0}")]
    KeyLeaderNotFound(String),

    #[error("key not found")]
    KeyNotFound(),

    #[error("unexpected version id")]
    UnexpectedVersionId(),

    #[error("session does not exist")]
    SessionDoesNotExist(),

    #[error("retryable")]
    InternalRetryable(),
}