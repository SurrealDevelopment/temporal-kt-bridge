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

/// tonic labels its local transport deadline CANCELLED. A server's cancellation has no
/// TimeoutExpired source and must keep its original code, even if its message is identical.
pub(crate) fn grpc_status_code(status: &tonic::Status) -> i32 {
    if status.code() == tonic::Code::Cancelled {
        let mut cause = std::error::Error::source(status);
        while let Some(error) = cause {
            if error.is::<tonic::TimeoutExpired>() {
                return tonic::Code::DeadlineExceeded as i32;
            }
            cause = error.source();
        }
    }
    status.code() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_local_transport_timeouts_are_deadline_exceeded() {
        let local = tonic::Status::from_error(Box::new(tonic::TimeoutExpired(())));
        assert_eq!(local.code(), tonic::Code::Cancelled);
        assert_eq!(
            grpc_status_code(&local),
            tonic::Code::DeadlineExceeded as i32
        );
        let server = tonic::Status::cancelled(local.message());
        assert_eq!(grpc_status_code(&server), tonic::Code::Cancelled as i32);
    }
}
