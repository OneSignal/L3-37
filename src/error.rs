#[derive(Debug, Fail)]
pub enum InternalError {
    #[fail(display = "unknown error: {}", _0)]
    Other(String),

    #[fail(display = "Tried to get a connection from the pool, but all connections were invalid")]
    AllConnectionsInvalid,

    #[fail(display = "Timed out waiting for a connection to become available")]
    TimedOut,
}

/// Error type returned by this module
#[derive(Debug, Fail)]
pub enum Error<E: failure::Fail> {
    /// Error coming from the connection pooling itself
    #[fail(display = "l337 internal error: {}", _0)]
    Internal(InternalError),

    /// Error from the connection manager or the underlying client
    #[fail(display = "l337 manager error: {}", _0)]
    External(E),
}

impl<E> From<tokio::time::error::Elapsed> for Error<E>
where
    E: failure::Fail,
{
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Internal(InternalError::TimedOut)
    }
}
