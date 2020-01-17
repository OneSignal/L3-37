//! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]

extern crate futures;
pub extern crate l337;
extern crate tokio;
pub extern crate tokio_postgres;

#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;

use futures::channel::oneshot;
use std::{
    convert::{AsMut, AsRef},
    ops::{Deref, DerefMut},
};
use tokio::spawn;
use tokio_postgres::error::Error;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Client, Socket,
};

use std::fmt;

pub struct AsyncConnection {
    pub client: Client,
    broken: bool,
    receiver: oneshot::Receiver<bool>,
}

impl Deref for AsyncConnection {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for AsyncConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl AsMut<Client> for AsyncConnection {
    fn as_mut(&mut self) -> &mut Client {
        &mut self.client
    }
}

impl AsRef<Client> for AsyncConnection {
    fn as_ref(&self) -> &Client {
        &self.client
    }
}

/// A `ManageConnection` for `tokio_postgres::Connection`s.
pub struct PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
{
    config: tokio_postgres::Config,
    make_tls_connect: T,
}

impl<T> PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
{
    /// Create a new `PostgresConnectionManager`.
    pub fn new(config: tokio_postgres::Config, make_tls_connect: T) -> Self {
        Self {
            config,
            make_tls_connect,
        }
    }
}

#[async_trait]
impl<T> l337::ManageConnection for PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
    T::Stream: Send + Sync,
    T::TlsConnect: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = AsyncConnection;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, l337::Error<Self::Error>> {
        let (client, connection) = self
            .config
            .connect(self.make_tls_connect.clone())
            .await
            .map_err(|e| l337::Error::External(e))?;

        let (sender, receiver) = oneshot::channel();
        spawn(async move {
            if let Err(_) = connection.await {
                sender
                    .send(true)
                    .unwrap_or_else(|e| panic!("failed to send shutdown notice: {}", e));
            }
        });

        Ok(AsyncConnection {
            broken: false,
            client,
            receiver,
        })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), l337::Error<Self::Error>> {
        // If we can execute this without erroring, we're definitely still connected to the database
        conn.simple_query("")
            .await
            .map_err(|e| l337::Error::External(e))?;

        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        if conn.broken {
            return true;
        }

        // Use try_recv() as `has_broken` can be called via Drop and not have a
        // future Context to poll on.
        // https://docs.rs/futures/0.3.1/futures/channel/oneshot/struct.Receiver.html#method.try_recv
        match conn.receiver.try_recv() {
            // If we get any message, the connection task stopped, which means this connection is
            // now dead
            Ok(Some(_)) => {
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
        // Error::io(io::ErrorKind::TimedOut.into())
    }
}

impl<T> fmt::Debug for PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use l337::{Config, Pool};
    use std::time::Duration;
    use tokio::time::delay_for;

    #[tokio::test]
    async fn it_works() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        );

        let config: Config = Default::default();
        let pool = Pool::new(mngr, config).await.unwrap();
        let conn = pool.connection().await.unwrap();
        let select = conn.prepare("SELECT 1::INT4").await.unwrap();

        let rows = conn.query(&select, &[]).await.unwrap();

        for row in rows {
            assert_eq!(1, row.get(0));
        }
    }

    #[tokio::test]
    async fn it_allows_multiple_queries_at_the_same_time() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        );

        let config: Config = Default::default();
        let pool = Pool::new(mngr, config).await.unwrap();

        let q1 = async {
            let conn = pool.connection().await.unwrap();
            let select = conn.prepare("SELECT 1::INT4").await.unwrap();
            let rows = conn.query(&select, &[]).await.unwrap();

            for row in rows {
                assert_eq!(1, row.get(0));
            }

            delay_for(Duration::from_secs(5)).await;

            conn
        };

        let q2 = async {
            let conn = pool.connection().await.unwrap();
            let select = conn.prepare("SELECT 2::INT4").await.unwrap();
            let rows = conn.query(&select, &[]).await.unwrap();

            for row in rows {
                assert_eq!(2, row.get(0));
            }

            delay_for(Duration::from_secs(5)).await;

            conn
        };

        futures::join!(q1, q2);
    }

    #[tokio::test]
    async fn it_reuses_connections() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        );

        let config: Config = Default::default();
        let pool = Pool::new(mngr, config).await.unwrap();
        let q1 = async {
            let conn = pool.connection().await.unwrap();
            let select = conn.prepare("SELECT 1::INT4").await.unwrap();
            let rows = conn.query(&select, &[]).await.unwrap();

            for row in rows {
                assert_eq!(1, row.get(0));
            }
        };

        q1.await;

        // This delay is required to ensure that the connection is returned to
        // the pool after Drop runs. Because Drop spawns a future that returns
        // the connection to the pool.
        delay_for(Duration::from_millis(500)).await;

        let q2 = async {
            let conn = pool.connection().await.unwrap();
            let select = conn.prepare("SELECT 2::INT4").await.unwrap();
            let rows = conn.query(&select, &[]).await.unwrap();

            for row in rows {
                assert_eq!(2, row.get(0));
            }
        };

        let q3 = async {
            let conn = pool.connection().await.unwrap();
            let select = conn.prepare("SELECT 3::INT4").await.unwrap();
            let rows = conn.query(&select, &[]).await.unwrap();

            for row in rows {
                assert_eq!(3, row.get(0));
            }
        };

        futures::join!(q2, q3);

        assert_eq!(pool.total_conns().await, 2);
    }
}
