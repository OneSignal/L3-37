use futures::future::{self, Future};
use inner::ConnectionPool;
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
    pub conn: Option<Live<C::Connection>>,
    // In a normal case this is always Some, but it can be none if constructed from the
    // new_unpooled constructor.
    pub pool: Option<Arc<ConnectionPool<C>>>,
}
