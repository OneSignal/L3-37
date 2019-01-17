//! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]

extern crate futures;
pub extern crate l337;
extern crate redis;
extern crate tokio;

use futures::Future;
use redis::async::{ConnectionLike, SharedConnection};
use redis::{Client, IntoConnectionInfo, RedisError};

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

/// Unfortunately a required struct to make the redis-rs api work.
/// As a todo, we might want to consider improving the redis-rs api or switching to a different lib
/// Looking at the tests will make why this struct is needed clear, but basically it requires
/// an owned version of conn so we can't use deref to make everything work nice.
pub struct AsyncConnection(pub l337::Conn<RedisConnectionManager>);

impl ConnectionLike for AsyncConnection {
    fn req_packed_command(
        self,
        cmd: Vec<u8>,
    ) -> Box<Future<Item = (Self, redis::Value), Error = RedisError> + Send> {
        let conn = self.0.clone();
        Box::new(
            conn.req_packed_command(cmd)
                .map(move |(_conn, value)| (self, value)),
        )
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> Box<Future<Item = (Self, Vec<redis::Value>), Error = RedisError> + Send> {
        let conn = self.0.clone();
        Box::new(
            conn.req_packed_commands(cmd, offset, count)
                .map(move |(_conn, value)| (self, value)),
        )
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }
}

impl l337::ManageConnection for RedisConnectionManager {
    type Connection = SharedConnection;
    type Error = RedisError;

    fn connect(
        &self,
    ) -> Box<Future<Item = Self::Connection, Error = l337::Error<Self::Error>> + 'static + Send>
    {
        Box::new(
            self.client
                .get_shared_async_connection()
                .map_err(l337::Error::External),
        )
    }

    fn is_valid(
        &self,
        conn: Self::Connection,
    ) -> Box<Future<Item = (), Error = l337::Error<Self::Error>>> {
        Box::new(
            redis::cmd("PING")
                .query_async::<_, ()>(conn)
                .map(|_| ())
                .map_err(|e| l337::Error::External(e)),
        )
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }

    fn timed_out(&self) -> l337::Error<Self::Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use l337::{Config, Pool};
    use tokio::runtime::Runtime;

    #[test]
    fn it_works() {
        let mngr = RedisConnectionManager::new("redis://localhost:6370/0").unwrap();

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();

        let future = Pool::new(mngr, config).and_then(|pool| {
            pool.connection().and_then(|conn| {
                redis::cmd("PING")
                    .query_async::<_, ()>(AsyncConnection(conn))
                    .map(|_| println!("done ping"))
                    .map_err(|e| l337::Error::External(e))
            })
        });

        runtime.block_on(future).expect("could not run");
    }
}
