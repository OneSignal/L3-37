///! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]
extern crate futures;
pub extern crate l337;
extern crate tokio;
pub extern crate tokio_postgres;

#[macro_use]
extern crate log;

//use futures::sync::oneshot;
use async_trait::async_trait;
use futures::{stream::Stream, FutureExt};
use std::future::Future;
use tokio::executor::spawn;
use tokio::sync::oneshot;
use tokio_postgres::error::Error;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Client, Socket,
};

use std::fmt;

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
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send + Sync,
{
    type Connection = tokio_postgres::Client;
    type Error = Error;
    async fn connect(&self) -> Result<Self::Connection, l337::Error<Self::Error>> {
        println!("Spawning postgres connection");
        let result = self.config.connect(self.make_tls_connect.clone()).await;
        println!("Connected to postgres db");
        let (client, connection) = result.map_err(|err| l337::Error::External(err))?;
        spawn(connection.map(|_| {}));
        println!("Returning connection");
        Ok(client)
    }
    async fn is_valid(&self, conn: Self::Connection) -> Result<(), l337::Error<Self::Error>> {
        // If we can execute this without erroring, we're definitely still connected to the database
        conn.simple_query("")
            .await
            .map_err(|e| l337::Error::External(e))?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
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
    use futures::{join, Stream};
    use l337::{Config, Pool};
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::runtime::current_thread::Runtime;

    #[test]
    fn it_works() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        );

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        runtime.block_on(async {
            let pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>> =
                Pool::new(mngr, config).await.unwrap();
            let conn = pool.connection().await.unwrap();
            let select = conn.prepare("SELECT 1::INT4").await.unwrap();
            let rows = conn.query(&select, &[]).await.unwrap();
            for row in rows {
                assert_eq!(1, row.get::<_, i32>(0));
                ()
            }
        });
    }

    #[test]
    fn it_allows_multiple_queries_at_the_same_time() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        );

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        runtime.block_on(async {
            let pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>> =
                Pool::new(mngr, config).await.unwrap();
            let q1 = async {
                let conn = pool.connection().await.unwrap();
                let select = conn.prepare("SELECT 1::INT4").await.unwrap();
                let rows = conn.query(&select, &[]).await.unwrap();
                for row in rows {
                    assert_eq!(1, row.get::<_, i32>(0));
                    ()
                }
                //sleep(Duration::from_secs(5));
            };
            let q2 = async {
                let conn = pool.connection().await.unwrap();
                println!("Q2 got connection");
                let select = conn.prepare("SELECT 2::INT4").await.unwrap();
                let rows = conn.query(&select, &[]).await.unwrap();
                for row in rows {
                    assert_eq!(2, row.get::<_, i32>(0));
                    ()
                }

                //sleep(Duration::from_secs(5));
            };
            //join!(q1, q2) // This is hanging for some reason. Not sure why
            q1.await;
            q2.await;
        });
    }

    #[test]
    fn it_reuses_connections() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        );

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        runtime.block_on(async {
            let pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>> =
                Pool::new(mngr, config).await.unwrap();
            let q1 = async {
                let conn = pool.connection().await.unwrap();
                let select = conn.prepare("SELECT 1::INT4").await.unwrap();
                let rows = conn.query(&select, &[]).await.unwrap();
                for row in rows {
                    assert_eq!(1, row.get::<_, i32>(0));
                    ()
                }
                //sleep(Duration::from_secs(5));
            };
            let q2 = async {
                let conn = pool.connection().await.unwrap();
                println!("Q2 got connection");
                let select = conn.prepare("SELECT 2::INT4").await.unwrap();
                let rows = conn.query(&select, &[]).await.unwrap();
                for row in rows {
                    assert_eq!(2, row.get::<_, i32>(0));
                    ()
                }
                //sleep(Duration::from_secs(5));
            };
            let q3 = async {
                let conn = pool.connection().await.unwrap();
                println!("Q2 got connection");
                let select = conn.prepare("SELECT 3::INT4").await.unwrap();
                let rows = conn.query(&select, &[]).await.unwrap();
                for row in rows {
                    assert_eq!(3, row.get::<_, i32>(0));
                    ()
                }
                //sleep(Duration::from_secs(5));
            };
            //join!(q1, q2, q3) // This is hanging for some reason. Not sure why
            q1.await;
            q2.await;
            q3.await;
        });
    }
}
