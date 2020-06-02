use std::time::Duration;

/// Configuration for the connection pool
#[derive(Debug)]
pub struct Config {
    pub(crate) min_size: usize,
    pub(crate) max_size: usize,
    pub(crate) idle_queue_size: usize,
    pub(crate) test_on_check_out: bool,
    pub(crate) connect_timeout: Option<Duration>,
}

impl Config {
    /// Create a new configuration object with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout period for starting up a new connection. If it takes longer
    /// than the specified time to wait for a connection to become available, it
    /// will fail.
    ///
    /// By default, there is no timeout limit on getting a new connection.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// If true, the health of a connection will be verified via a call to
    /// `ConnectionManager::is_valid` before it is checked out of the pool.
    ///
    /// Defaults to true.
    pub fn test_on_check_out(mut self, test_on_check_out: bool) -> Self {
        self.test_on_check_out = test_on_check_out;
        self
    }

    /// Minimum number of connections in the pool. The pool will be initialied with this number of
    /// connections
    ///
    /// Defaults to 1 connection.
    pub fn min_size(mut self, min_size: usize) -> Self {
        self.min_size = min_size;
        self
    }

    /// Max number of connections to keep in the pool
    ///
    /// Defaults to 10 connections.
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self.idle_queue_size = max_size * IDLE_QUEUE_SIZE_FACTOR;
        self
    }
}

/// Initial minimum connection count(s) in the pool.
const MIN_SIZE: usize = 1;
/// Maximum connections, both active and idle, in the pool.
const MAX_SIZE: usize = 10;
/// Idle connection queue size factor applied to the MAX_SIZE above for the idle
/// connection queue.  This technically should be 1, to make the idle queue size
/// equal to the maximum number of connection in the pool, as we won't allow the
/// total number of connections, both live and idel, exceed to maximum size.
const IDLE_QUEUE_SIZE_FACTOR: usize = 2;

impl Default for Config {
    fn default() -> Self {
        Config {
            max_size: MAX_SIZE,
            min_size: MIN_SIZE,
            idle_queue_size: MAX_SIZE * IDLE_QUEUE_SIZE_FACTOR,
            test_on_check_out: true,
            connect_timeout: None,
        }
    }
}
