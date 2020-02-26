/// Configuration for the connection pool
#[derive(Debug)]
pub struct Config {
    pub(crate) min_size: usize,
    pub(crate) max_size: usize,
    pub(crate) test_on_check_out: bool,
}

impl Config {
    /// Create a new configuration object with default values
    pub fn new() -> Self {
        Self::default()
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
    pub fn min_size(mut self, min_size: usize) -> Self {
        self.min_size = min_size;
        self
    }

    /// Max number of connections to keep in the pool
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_size: 10,
            min_size: 1,
            test_on_check_out: true,
        }
    }
}
