#![deny(missing_docs)]

//! Connection pooling library for tokio.
//!
//! Any connection type that implements the `ManageConnection` trait can be used with this libary.

extern crate crossbeam;
extern crate futures;
extern crate tokio;

mod conn;
mod inner;
mod manage_connection;
mod queue;

use futures::future::{self, Future};
use futures::stream;
use futures::sync::oneshot;
use futures::Stream;
use std::sync::Arc;

pub use manage_connection::ManageConnection;

use conn::{Conn, ConnFuture};
use inner::ConnectionPool;
use queue::{Live, Queue};

/// General connection pool
pub struct Pool<C: ManageConnection> {
    conn_pool: Arc<ConnectionPool<C>>,
}

/// Configuration for the connection pool
#[derive(Debug)]
pub struct Config {
    /// Minimum number of connections in the pool. The pool will be initialied with this number of
    /// connections
    min_size: usize,
    /// Max number of connections to keep in the pool
    max_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_size: 10,
            min_size: 1,
        }
    }
}

impl<C: ManageConnection> Pool<C> {
    /// Creates a new connection pool
    ///
    /// The returned future will resolve to the pool if successful, which can then be used
    /// immediately.
    pub fn new(manager: C, config: Config) -> Box<Future<Item = Pool<C>, Error = C::Error>> {
        assert!(
            config.max_size >= config.min_size,
            "max_size of pool must be greater than or equal to the min_size"
        );

        let conns = stream::futures_unordered(
            ::std::iter::repeat(&manager)
                .take(config.min_size)
                .map(|c| c.connect()),
        );

        // Fold the connections we are creating into a Queue object
        let conns = conns.fold::<_, _, Result<_, _>>(Queue::new(), |conns, conn| {
            conns.new_conn(Live::new(conn));
            Ok(conns)
        });

        // Set up the pool once the connections are established
        Box::new(conns.and_then(move |conns| {
            let conn_pool = Arc::new(ConnectionPool::new(conns, manager, config));

            Ok(Pool { conn_pool })
        }))
    }

    /// Returns a future that resolves to a connection from the pool.
    ///
    /// If there are connections that are available to be used, the future will resolve immediately,
    /// otherwise, the connection will be in a pending state until a future is returned to the pool.
    ///
    /// This **does not** implement any timeout functionality. Timeout functionality can be added
    /// by calling `.timeout` on the returned future.
    pub fn connection(&self) -> ConnFuture<Conn<C>, C::Error> {
        if let Some(conn) = self.conn_pool.get_connection() {
            future::Either::A(future::ok(Conn {
                conn: Some(conn),
                pool: Arc::clone(&self.conn_pool),
            }))
        } else {
            if let Some(conn_future) = self.conn_pool.try_spawn_connection() {
                let pool_clone = Arc::clone(&self.conn_pool);
                return future::Either::B(Box::new(conn_future.map(|conn| Conn {
                    conn: Some(conn),
                    pool: Some(pool_clone),
                })));
            }
            // Have the pool notify us of the connection
            let (tx, rx) = oneshot::channel();
            self.conn_pool.notify_of_connection(tx);

            // Prepare the future which will wait for a free connection
            let pool = Arc::clone(&self.conn_pool);
            future::Either::B(Box::new(
                rx.map(|conn| Conn {
                    conn: Some(conn),
                    pool: pool,
                }).map_err(|_err| unimplemented!()),
            ))
        }
    }

    /// Returns the number of idle (ready to be used) connections in the pool. Not incredibly useful
    /// for general runtime usuage, but can be very useful for debugging or tests.
    pub fn idle_conns(&self) -> usize {
        self.conn_pool.idle_conns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::prelude::FutureExt;
    use tokio::runtime::current_thread::Runtime;

    #[derive(Debug)]
    pub struct DummyManager {}

    impl ManageConnection for DummyManager {
        type Connection = ();
        type Error = ();

        fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'static> {
            Box::new(future::ok(()))
        }

        fn is_valid(&self, _conn: Self::Connection) -> Box<Future<Item = (), Error = Self::Error>> {
            unimplemented!()
        }
        /// Produce an error representing a connection timeout.
        fn timed_out(&self) -> Self::Error {
            unimplemented!()
        }
    }

    #[test]
    fn simple_pool_creation_and_connection() {
        let mngr = DummyManager {};
        let config: Config = Default::default();

        let future = Pool::new(mngr, config).and_then(|pool| {
            pool.connection().and_then(|conn| {
                if let Some(Live {
                    conn: (),
                    live_since: _,
                }) = conn.conn
                {
                    Ok(())
                } else {
                    panic!("connection is not correct type: {:?}", conn)
                }
            })
        });

        Runtime::new()
            .expect("could not run")
            .block_on(future)
            .expect("could not run");
    }

    #[test]
    fn it_returns_a_non_resolved_future_when_over_pool_limit() {
        let mngr = DummyManager {};
        let config: Config = Config {
            max_size: 2,
            min_size: 2,
        };

        // pool is of size , we try to get 2 connections so the second one will never resolve
        let future = Pool::new(mngr, config).and_then(|pool| {
            // Forget the values so we don't drop them, and return them back to the pool
            ::std::mem::forget(pool.connection());
            pool.connection()
                .timeout(Duration::from_millis(10))
                .then(|r| match r {
                    Ok(_) => panic!("didn't timeout"),
                    Err(err) => {
                        if err.is_elapsed() {
                            Ok(())
                        } else {
                            panic!("didn't timeout")
                        }
                    }
                })
        });

        Runtime::new()
            .expect("could not run")
            .block_on(future)
            .expect("could not run");
    }

    #[test]
    fn it_allocates_new_connections_up_to_max_size() {
        let mngr = DummyManager {};
        let config: Config = Config {
            max_size: 2,
            min_size: 1,
        };

        // pool is of size 1, but is allowed to generate new connections up to 2.
        // When we try 2 connections, they should both pass without timing out
        let future = Pool::new(mngr, config).and_then(|pool| {
            // Forget the values so we don't drop them, and return them back to the pool
            ::std::mem::forget(pool.connection());
            let f1 = pool
                .connection()
                .timeout(Duration::from_millis(10))
                .then(|r| match r {
                    Ok(conn) => {
                        ::std::mem::forget(conn);
                        Ok::<(), ()>(())
                    }
                    Err(err) => {
                        if err.is_elapsed() {
                            panic!("second connection timed out")
                        } else {
                            Ok(())
                        }
                    }
                });

            // The third connection should timeout though, as we're only allowed to go up to 2
            let f2 = pool
                .connection()
                .timeout(Duration::from_millis(10))
                .then(|r| match r {
                    Ok(_) => panic!("third didn't timeout"),
                    Err(err) => {
                        if err.is_elapsed() {
                            Ok(())
                        } else {
                            panic!("third didn't timeout")
                        }
                    }
                });

            f1.join(f2)
        });

        Runtime::new()
            .expect("could not run")
            .block_on(future)
            .expect("could not run");
    }
}
