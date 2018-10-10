use crossbeam::queue::SegQueue;
use futures::sync::oneshot;
use futures::Future;
use std::sync::Arc;

use manage_connection::ManageConnection;
use queue::{Live, Queue};
use Config;
use Error;

// Most of this comes from c3po's inner module: https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/inner.rs
// with some additions and updates to work with modern versions of tokio

/// Inner connection pool. Handles creating and holding the connections, as well as keeping track of
/// futures that are waiting on connections.
#[derive(Debug)]
pub struct ConnectionPool<C: ManageConnection> {
    /// Queue of connections in the pool
    conns: Arc<Queue<C::Connection>>,
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
            conns: Arc::new(conns),
            waiting: SegQueue::new(),
            manager,
            config,
        }
    }

    /// Returns a connection if there is one ready. This does not implement any kind of waiting or
    /// backlog mechanism, for that, see `Pool.connection`
    pub fn get_connection(&self) -> Option<Live<C::Connection>> {
        self.conns.get()
    }

    /// Adds a "waiter" to the queue of waiting futures. When a new connection becomes available,
    /// the oneshot will be called with a new connection
    pub fn notify_of_connection(&self, tx: oneshot::Sender<Live<C::Connection>>) {
        self.waiting.push(tx);
    }

    /// The total number of connections in the pool.
    pub fn total_conns(&self) -> usize {
        self.conns.total()
    }

    /// The number of idle connections in the pool.
    pub fn idle_conns(&self) -> usize {
        self.conns.idle()
    }

    /// Attempt to spawn a new connection. If we're not already over the max number of connections,
    /// a future will be returned that resolves to the new connection.
    /// Otherwise, None will be returned
    pub(crate) fn try_spawn_connection(
        &self,
    ) -> Option<Box<Future<Item = Live<C::Connection>, Error = Error<C::Error>>>> {
        if let Some(_) = self.conns.safe_increment(self.config.max_size) {
            let conns = Arc::clone(&self.conns);
            Some(Box::new(self.manager.connect().then(
                move |result| match result {
                    Ok(conn) => Ok(Live::new(conn)),
                    Err(err) => {
                        // if we weren't able to make a new connection, we need to decrement
                        // connections, since we preincremented the connection count for this  one
                        conns.decrement();
                        Err(err)
                    }
                },
            )))
        } else {
            None
        }
    }

    /// Receive a connection back to be stored in the pool. This could have one
    /// of two outcomes:
    /// * The connection will be passed to a waiting future, if any exist.
    /// * The connection will be put back into the connection pool.
    pub fn store(&self, conn: Live<C::Connection>) {
        // first attempt to send it to any waiting requests
        let mut conn = conn;
        while let Some(waiting) = self.waiting.try_pop() {
            conn = match waiting.send(conn) {
                Ok(_) => return,
                Err(conn) => conn,
            };
        }

        // If there are no waiting requests & we aren't over the max idle
        // connections limit, attempt to store it back in the pool
        self.conns.store(conn);
    }
}
