//! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]

extern crate futures;
pub extern crate l337;
extern crate redis;
extern crate tokio;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;

use futures::{
    channel::oneshot,
    future::{self, BoxFuture},
};
use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::{Client, Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, Value};

use std::{
    convert::{AsMut, AsRef},
    ops::{Deref, DerefMut},
};

type Result<T> = std::result::Result<T, RedisError>;

/// A `ManageConnection` for `RedisConnections`s.
#[derive(Debug)]
pub struct RedisConnectionManager {
    client: redis::Client,
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    pub fn new(params: impl IntoConnectionInfo) -> Result<RedisConnectionManager> {
        Ok(RedisConnectionManager {
            client: Client::open(params)?,
        })
    }
}

pub struct AsyncConnection {
    pub conn: MultiplexedConnection,
    done_rx: oneshot::Receiver<()>,
    drop_tx: Option<oneshot::Sender<()>>,
    broken: bool,
}

// Connections can be dropped when they report an error from is_valid, or return
// true from has_broken. The channel is used here to ensure that the async
// driver task spawned in RedisConnectionManager::connect is ended.
impl Drop for AsyncConnection {
    fn drop(&mut self) {
        if let Some(drop_tx) = self.drop_tx.take() {
            let _ = drop_tx.send(());
        }
    }
}

impl Deref for AsyncConnection {
    type Target = MultiplexedConnection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for AsyncConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl AsMut<MultiplexedConnection> for AsyncConnection {
    fn as_mut(&mut self) -> &mut MultiplexedConnection {
        &mut self.conn
    }
}

impl AsRef<MultiplexedConnection> for AsyncConnection {
    fn as_ref(&self) -> &MultiplexedConnection {
        &self.conn
    }
}

impl ConnectionLike for AsyncConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.conn.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.conn.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.conn.get_db()
    }
}

/// Rewite of redis::transaction for use with an async connection. It is assumed
/// that the fn's return value will be the return value of Pipeline::query_async.
/// Returning None from this fn will cause it to be re-run, as that is the value
/// returned from Pipeline::query_async when run in atomic mode, and the watched
/// keys are modified somewhere else.
///
/// ```rust,no_run
/// use redis::AsyncCommands;
/// use futures::prelude::*;
/// # async fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_async_connection().await.unwrap();
/// let key = "the_key";
/// let mut count = 0i32;
/// let (new_val,) : (isize,) = l337_redis::async_transaction(&mut con, &[key], &mut count, |con, pipe, count_ref| async move {
///     *count_ref += 1;
///     let old_val : isize = con.get(key).await?;
///     pipe
///         .set(key, old_val + 1).ignore()
///         .get(key)
///         .query_async(con)
///         .await
/// }.boxed()).await?;
/// println!("The incremented number is: {}", new_val);
/// # Ok(()) }
/// ```
pub async fn async_transaction<C, K, T, F, Args>(
    con: &mut C,
    keys: &[K],
    args: &mut Args,
    func: F,
) -> redis::RedisResult<T>
where
    C: ConnectionLike,
    K: redis::ToRedisArgs,
    F: for<'a> FnMut(
        &'a mut C,
        &'a mut Pipeline,
        &'a mut Args,
    ) -> BoxFuture<'a, redis::RedisResult<Option<T>>>,
{
    let mut func = func;
    loop {
        redis::cmd("WATCH")
            .arg(keys)
            .query_async::<_, ()>(&mut *con)
            .await?;

        let mut p = redis::pipe();
        let response: Option<T> = func(con, p.atomic(), args).await?;
        match response {
            None => {
                continue;
            }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                redis::cmd("UNWATCH")
                    .query_async::<_, ()>(&mut *con)
                    .await?;
                return Ok(response);
            }
        }
    }
}

#[async_trait]
impl l337::ManageConnection for RedisConnectionManager {
    type Connection = AsyncConnection;
    type Error = RedisError;

    async fn connect(&self) -> std::result::Result<Self::Connection, l337::Error<Self::Error>> {
        debug!("connect: try redis connection");
        let (connection, future) = self
            .client
            .create_multiplexed_tokio_connection()
            .await
            .map_err(l337::Error::External)?;

        let (done_tx, done_rx) = oneshot::channel();
        let (drop_tx, drop_rx) = oneshot::channel();

        tokio::spawn(async move {
            debug!("connect: spawn future backing redis connection");
            futures::pin_mut!(future, drop_rx);

            future::select(future, drop_rx).await;
            debug!("Future backing redis connection ended, future calls to this redis connection will fail");

            // If there was an error sending this message, it means that the
            // RedisConnectionManager has died, and there is no need to notify
            // it that this connection has died.
            let _ = done_tx.send(());
        });

        debug!("connect: redis connection established");
        Ok(AsyncConnection {
            conn: connection,
            broken: false,
            done_rx,
            drop_tx: Some(drop_tx),
        })
    }

    async fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> std::result::Result<(), l337::Error<Self::Error>> {
        let result = redis::cmd("PING")
            .query_async::<_, ()>(conn)
            .await
            .map_err(l337::Error::External);

        if result.is_err() {
            conn.broken = true;
        }

        result
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        if conn.broken {
            return true;
        }

        // Use try_recv() as `has_broken` can be called via Drop and not have a
        // future Context to poll on.
        // https://docs.rs/futures/0.3.1/futures/channel/oneshot/struct.Receiver.html#method.try_recv
        match conn.done_rx.try_recv() {
            // If we get any message, the connection task stopped, which means this connection is
            // now dead
            Ok(Some(())) => {
                conn.broken = true;
                true
            }
            // If the future isn't ready, then we haven't sent a value which means the future is
            // still successfully running
            Ok(None) => false,
            // This can happen if the future that the connection was
            // spawned in panicked or was dropped.
            Err(err) => {
                warn!("cannot receive from connection future - err: {}", err);
                conn.broken = true;
                true
            }
        }
    }

    fn timed_out(&self) -> l337::Error<Self::Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use l337::{Config, Pool};

    #[tokio::test]
    async fn it_works() {
        let mngr = RedisConnectionManager::new("redis://redis:6379/0").unwrap();

        let config: Config = Default::default();

        let pool = Pool::new(mngr, config).await.unwrap();
        let mut conn = pool.connection().await.unwrap();
        redis::cmd("PING")
            .query_async::<_, ()>(&mut *conn)
            .await
            .unwrap();

        println!("done ping")
    }
}
