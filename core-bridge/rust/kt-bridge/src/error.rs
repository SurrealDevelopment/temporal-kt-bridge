//! Bridge errors and their wire codes.

use crate::abi::*;

#[derive(Debug, thiserror::Error)]
pub enum KtError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The handle was freed, or never existed. Returned rather than dereferenced, which is what
    /// makes use-after-free an error code here instead of undefined behaviour.
    #[error("stale handle")]
    StaleHandle,
    #[error("handle is not of the expected kind")]
    WrongHandleKind,
    #[error("runtime is shutting down")]
    Shutdown,
    #[error("worker already shut down")]
    WorkerShutDown,
    #[error("cancelled")]
    Cancelled,
    #[error("buffer too small")]
    BufferTooSmall,
    #[error("{0}")]
    Failed(String),
}

impl KtError {
    pub fn code(&self) -> i32 {
        match self {
            KtError::InvalidArgument(_) => KT_ERR_INVALID_ARGUMENT,
            KtError::StaleHandle => KT_ERR_STALE_HANDLE,
            KtError::WrongHandleKind => KT_ERR_WRONG_HANDLE_KIND,
            KtError::Shutdown => KT_ERR_SHUTDOWN,
            KtError::WorkerShutDown => KT_ERR_WORKER_SHUT_DOWN,
            KtError::Cancelled => KT_ERR_CANCELLED,
            KtError::BufferTooSmall => KT_ERR_BUFFER_TOO_SMALL,
            KtError::Failed(_) => KT_ERR_FAILED,
        }
    }
}

impl From<anyhow::Error> for KtError {
    fn from(value: anyhow::Error) -> Self {
        KtError::Failed(format!("{value:#}"))
    }
}

impl From<prost::DecodeError> for KtError {
    fn from(value: prost::DecodeError) -> Self {
        KtError::InvalidArgument(format!("could not decode protobuf: {value}"))
    }
}

pub type KtResult<T = ()> = Result<T, KtError>;
