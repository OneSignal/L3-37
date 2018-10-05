// #![deny(missing_docs)]

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

pub struct Pool<C: ManageConnection> {
    conn_pool: Arc<ConnectionPool<C>>,
}

impl<C: ManageConnection> Pool<C> {
    pub fn new(manager: C) -> Box<Future<Item = Pool<C>, Error = C::Error>> {
        // TODO: remove hard coding from take
        let conns =
            stream::futures_unordered(::std::iter::repeat(&manager).take(2).map(|c| c.connect()));

        // Fold the connections we are creating into a Queue object
        let conns = conns.fold::<_, _, Result<_, _>>(Queue::new(), |conns, conn| {
            conns.new_conn(Live::new(conn));
            Ok(conns)
        });

        // Set up the pool once the connections are established
        Box::new(conns.and_then(move |conns| {
            let conn_pool = Arc::new(ConnectionPool::new(conns, manager));

            Ok(Pool { conn_pool })
        }))
    }

    pub fn connection(&self) -> ConnFuture<Conn<C>, C::Error> {
        if let Some(conn) = self.conn_pool.get_connection() {
            future::Either::A(future::ok(Conn {
                conn: Some(conn),
                pool: Some(Arc::clone(&self.conn_pool)),
            }))
        } else {
            //Have the pool notify us of the connection
            let (tx, rx) = oneshot::channel();
            self.conn_pool.notify_of_connection(tx);

            // Prepare the future which will wait for a free connection
            let pool = self.conn_pool.clone();
            future::Either::B(Box::new(
                rx.map(|conn| Conn {
                    conn: Some(conn),
                    pool: Some(pool),
                }).map_err(|_err| unimplemented!()),
            ))
        }
    }

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

        // fn is_valid(
        //     &self,
        //     _conn: Self::Connection,
        // ) -> Box<Future<Item = Self::Connection, Error = (Self::Error, Self::Connection)>> {
        //     unimplemented!()
        // }
        // /// Synchronously determine if the connection is no longer usable, if possible.
        // fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        //     unimplemented!()
        // }
        // /// Produce an error representing a connection timeout.
        fn timed_out(&self) -> Self::Error {
            unimplemented!()
        }
    }

    #[test]
    fn simple_pool_creation_and_connection() {
        let mngr = DummyManager {};

        let future = Pool::new(mngr).and_then(|pool| {
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

        // pool is of size 2, we try to get 3 connections so the third one will never resolve
        let future = Pool::new(mngr).and_then(|pool| {
            // Forget the values so we don't drop them, and return them back to the pool
            ::std::mem::forget(pool.connection());
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
}
