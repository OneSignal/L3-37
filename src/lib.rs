// #![deny(missing_docs)]

extern crate crossbeam;
extern crate futures;
extern crate tokio;

use crossbeam::queue::SegQueue;
use futures::future::{self, Future};
use futures::stream;
use futures::sync::oneshot;
use futures::Stream;
use std::sync::Arc;
use tokio::runtime::current_thread::Runtime;

use queue::{Live, Queue};

mod queue;

pub type ConnFuture<T, E> =
    future::Either<future::FutureResult<T, E>, Box<Future<Item = T, Error = E>>>;

/// A smart wrapper around a connection which stores it back in the pool
/// when it is dropped.
///
/// This can be dereferences to the `Service` instance this pool manages, and
/// also implements `Service` itself by delegating.
#[derive(Debug)]
pub struct Conn<C: ManageConnection> {
    conn: Option<Live<C::Connection>>,
    // In a normal case this is always Some, but it can be none if constructed from the
    // new_unpooled constructor.
    pool: Option<Arc<ConnectionPool<C>>>,
}

/// A trait which provides connection-specific functionality.
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;
    /// The error type returned by `Connection`s.
    type Error: Send + 'static;

    /// Attempts to create a new connection.
    fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'static>;
    /// Determines if the connection is still connected to the database.
    fn is_valid(
        &self,
        conn: Self::Connection,
    ) -> Box<Future<Item = Self::Connection, Error = (Self::Error, Self::Connection)>>;
    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;
    /// Produce an error representing a connection timeout.
    fn timed_out(&self) -> Self::Error;
}

#[derive(Debug)]
pub struct ConnectionPool<C: ManageConnection> {
    avail_conns: Queue<C::Connection>,
    waiting: SegQueue<oneshot::Sender<Live<C>>>,
    manager: C,
}

impl<C: ManageConnection> ConnectionPool<C> {
    pub fn new(conns: Queue<C::Connection>, manager: C) -> ConnectionPool<C> {
        ConnectionPool {
            avail_conns: conns,
            waiting: SegQueue::new(),
            manager,
        }
    }

    fn get_connection(&self) -> Option<Live<C::Connection>> {
        self.avail_conns.get()
    }
}

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
            unimplemented!()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct DummyManager {}

    impl ManageConnection for DummyManager {
        type Connection = ();
        type Error = ();

        fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'static> {
            Box::new(future::ok(()))
        }

        fn is_valid(
            &self,
            _conn: Self::Connection,
        ) -> Box<Future<Item = Self::Connection, Error = (Self::Error, Self::Connection)>> {
            unimplemented!()
        }
        /// Synchronously determine if the connection is no longer usable, if possible.
        fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
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

        let future = Pool::new(mngr).and_then(|pool| {
            pool.connection().and_then(|conn| {
                println!("hello");
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

}
