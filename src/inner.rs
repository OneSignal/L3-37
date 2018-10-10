use crossbeam::queue::SegQueue;
use futures::sync::oneshot;
use futures::Future;
use std::sync::{Arc, Mutex};

use manage_connection::ManageConnection;
use queue::{Live, Queue};
use Config;
use Error;

// Most of this comes from c3po's inner module: https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/inner.rs
// with some additions and updates to work with modern versions of tokio

/// Inner connection pool. Handles creating and holding the connections, as well as keeping track of
/// futures that are waiting on connections.
pub struct ConnectionPool<C: ManageConnection> {
    /// Queue of connections in the pool
    pub conns: Mutex<Arc<Queue<C::Connection>>>,
    /// Queue of oneshot's that are waiting to be given a new connection when the current pool is
    /// already saturated.
    waiting: SegQueue<oneshot::Sender<Live<C::Connection>>>,
    /// Connection manager used to create new connections as needed
    manager: C,
    /// Configuration for the pool
    config: Config,
}

impl<C: ManageConnection> ConnectionPool<C> {
    /// Creates a new connection pool
    pub fn new(conns: Queue<C::Connection>, manager: C, config: Config) -> ConnectionPool<C> {
        ConnectionPool {
            conns: Mutex::new(Arc::new(conns)),
            waiting: SegQueue::new(),
            manager,
            config,
        }
    }

    pub fn max_size(&self) -> usize {
        self.config.max_size
    }

    pub fn connect(&self) -> Box<Future<Item = C::Connection, Error = Error<C::Error>>> {
        self.manager.connect()
    }

    /// Adds a "waiter" to the queue of waiting futures. When a new connection becomes available,
    /// the oneshot will be called with a new connection
    pub fn notify_of_connection(&self, tx: oneshot::Sender<Live<C::Connection>>) {
        self.waiting.push(tx);
    }

    pub fn try_waiting(
        &self,
    ) -> Option<oneshot::Sender<Live<<C as ManageConnection>::Connection>>> {
        self.waiting.try_pop()
    }
}
