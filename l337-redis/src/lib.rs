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

use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::{Client, Cmd, IntoConnectionInfo, Pipeline, RedisError, RedisFuture, Value};

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
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.0.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.0.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }
}

#[async_trait]
impl l337::ManageConnection for RedisConnectionManager {
    type Connection = MultiplexedConnection;
    type Error = RedisError;

    async fn connect(&self) -> std::result::Result<Self::Connection, l337::Error<Self::Error>> {
        let (connection, future) = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(l337::Error::External)?;

        tokio::spawn(async move {
            future.await;
            error!("Connection backing future ended unexpectedly");
        });

        Ok(connection)
    }

    async fn is_valid(
        &self,
        mut conn: Self::Connection,
    ) -> std::result::Result<(), l337::Error<Self::Error>> {
        redis::cmd("PING")
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(l337::Error::External)?;

        Ok(())
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

    #[tokio::test]
    async fn it_works() {
        let mngr = RedisConnectionManager::new("redis://localhost:6379/0").unwrap();

        let config: Config = Default::default();

        let pool = Pool::new(mngr, config).await.unwrap();
        let conn = pool.connection().await.unwrap();
        redis::cmd("PING")
            .query_async::<_, ()>(&mut AsyncConnection(conn))
            .await
            .unwrap();

        println!("done ping")
    }
}
