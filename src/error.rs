#[derive(Debug, thiserror::Error)]
pub enum InternalError {
    #[error("unknown error: {0}")]
    Other(String),

    #[error("Tried to get a connection from the pool, but all connections were invalid")]
    AllConnectionsInvalid,

    #[error("Timed out waiting for a connection to become available")]
    TimedOut,
}

/// Error type returned by this module
#[derive(Debug, thiserror::Error)]
pub enum Error<E: std::error::Error + Send + Sync + 'static> {
    /// Error coming from the connection pooling itself
    #[error("l337 internal error: {0}")]
    Internal(InternalError),

    /// Error from the connection manager or the underlying client
    #[error("l337 manager error: {0}")]
    External(E),
}

impl<E> From<tokio::time::error::Elapsed> for Error<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Internal(InternalError::TimedOut)
    }
}
