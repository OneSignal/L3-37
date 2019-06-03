//! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]

extern crate futures;
pub extern crate l337;
extern crate tokio;
pub extern crate tokio_postgres;

use futures::sync::oneshot;
use futures::{Async, Future, Stream};
use tokio::executor::spawn;
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

/// A `ManageConnection` for `tokio_postgres::Connection`s.
pub struct PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
{
    config: String,
    make_tls_connect: T,
}

impl<T> PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
{
    /// Create a new `PostgresConnectionManager`.
    pub fn new(config: String, make_tls_connect: T) -> Self {
        Self {
            config,
            make_tls_connect,
        }
    }
}

impl<T> l337::ManageConnection for PostgresConnectionManager<T>
where
    T: 'static + MakeTlsConnect<Socket> + Clone + Send + Sync,
    T::Stream: Send + Sync,
    T::TlsConnect: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send + Sync,
{
    type Connection = AsyncConnection;
    type Error = Error;

    fn connect(
        &self,
    ) -> Box<Future<Item = Self::Connection, Error = l337::Error<Self::Error>> + 'static + Send>
    {
        Box::new(
            tokio_postgres::connect(&self.config, self.make_tls_connect.clone())
                .map(|(client, connection)| {
                    let (sender, receiver) = oneshot::channel();
                    spawn(connection.map_err(|_| {
                        sender
                            .send(true)
                            .unwrap_or_else(|e| panic!("failed to send shutdown notice: {}", e));
                    }));
                    AsyncConnection {
                        broken: false,
                        client,
                        receiver,
                    }
                })
                .map_err(|e| l337::Error::External(e)),
        )
    }

    fn is_valid(
        &self,
        mut conn: Self::Connection,
    ) -> Box<Future<Item = (), Error = l337::Error<Self::Error>>> {
        // If we can execute this without erroring, we're definitely still connected to the database
        Box::new(
            conn.client
                .simple_query("")
                .into_future()
                .map(|_| ())
                .map_err(|(e, _)| l337::Error::External(e)),
        )
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        if conn.broken {
            return true;
        }

        match conn.receiver.poll() {
            // If we get any message, the connection task stopped, which means this connection is
            // now dead
            Ok(Async::Ready(_)) => {
                conn.broken = true;
                true
            }
            // If the future isn't ready, then we haven't sent a value which means the future is
            // still successfully running
            Ok(Async::NotReady) => false,
            // This should never happen, we don't shutdown the future
            Err(err) => panic!("polling oneshot failed: {}", err),
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
    use futures::Stream;
    use l337::{Config, Pool};
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::runtime::current_thread::Runtime;

    #[test]
    fn it_works() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres".to_string(),
            tokio_postgres::NoTls,
        );

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        let future = Pool::new(mngr, config).and_then(|pool| {
            pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 1::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(1, row.get::<_, i32>(0));
                            Ok(())
                        })
                    })
                    .map(|connection| ((), connection))
                    .map_err(|e| l337::Error::External(e))
            })
        });

        runtime.block_on(future).expect("could not run");
    }

    #[test]
    fn it_allows_multiple_queries_at_the_same_time() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres".to_string(),
            tokio_postgres::NoTls,
        );

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        let future = Pool::new(mngr, config).and_then(|pool| {
            let q1 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 1::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(1, row.get::<_, i32>(0));
                            Ok(())
                        })
                    })
                    .map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
                    .map_err(|e| l337::Error::External(e))
            });

            let q2 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 2::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(2, row.get::<_, i32>(0));
                            Ok(())
                        })
                    })
                    .map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
                    .map_err(|e| l337::Error::External(e))
            });

            q1.join(q2)
        });

        runtime.block_on(future).expect("could not run");
    }

    #[test]
    fn it_reuses_connections() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres".to_string(),
            tokio_postgres::NoTls,
        );

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        let future = Pool::new(mngr, config).and_then(|pool| {
            let q1 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 1::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(1, row.get::<_, i32>(0));
                            Ok(())
                        })
                    })
                    .map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
                    .map_err(|e| l337::Error::External(e))
            });

            let q2 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 2::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(2, row.get::<_, i32>(0));
                            Ok(())
                        })
                    })
                    .map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
                    .map_err(|e| l337::Error::External(e))
            });

            let q3 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 3::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(3, row.get::<_, i32>(0));
                            Ok(())
                        })
                    })
                    .map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
                    .map_err(|e| l337::Error::External(e))
            });

            q1.join3(q2, q3)
        });

        runtime.block_on(future).expect("could not run");
    }
}
