use futures::future::{self, Future};
use inner::ConnectionPool;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use manage_connection::ManageConnection;
use queue::Live;

// From c3po, https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/lib.rs#L33

pub type ConnFuture<T, E> =
    future::Either<future::FutureResult<T, E>, Box<Future<Item = T, Error = E>>>;

// From c3po, https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/lib.rs#L40

/// A smart wrapper around a connection which stores it back in the pool
/// when it is dropped.
///
/// This can be dereferences to the `Service` instance this pool manages, and
/// also implements `Service` itself by delegating.
#[derive(Debug)]
pub struct Conn<C: ManageConnection> {
    /// Actual connection. This should never become a None variant under normal operation.
    /// This is an option so we can take the connection on drop, and push it back into the pool
    pub conn: Option<Live<C::Connection>>,
    /// Underlying pool. A reference is stored here so we can push the connection back into the
    /// pool on drop
    pub pool: Arc<ConnectionPool<C>>,
}

impl<C: ManageConnection> Deref for Conn<C> {
    type Target = C::Connection;
    fn deref(&self) -> &Self::Target {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<C: ManageConnection> DerefMut for Conn<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<C: ManageConnection> Drop for Conn<C> {
    fn drop(&mut self) {
        let conn = self.conn.take().unwrap();
        self.pool.store(conn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tests::DummyManager;
    use tokio::runtime::current_thread::Runtime;
    use Pool;

    #[test]
    fn conn_pushes_back_into_pool_after_drop() {
        let mngr = DummyManager {};

        let future = Pool::new(mngr).and_then(|pool| {
            assert_eq!(pool.idle_conns(), 2);

            pool.connection().and_then(move |conn| {
                assert_eq!(pool.idle_conns(), 1);

                ::std::mem::drop(conn);

                assert_eq!(pool.idle_conns(), 2);
                Ok(())
            })
        });

        Runtime::new()
            .expect("could not run")
            .block_on(future)
            .expect("could not run");
    }
}
