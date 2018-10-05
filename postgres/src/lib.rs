//! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]

extern crate futures;
pub extern crate l3_37;
pub extern crate postgres_shared;
extern crate tokio;
pub extern crate tokio_postgres;

use futures::Future;
use tokio_postgres::error::Error;
use tokio_postgres::params::ConnectParams;
use tokio_postgres::{Client, TlsMode};

use std::fmt;

type Result<T> = std::result::Result<T, Error>;

pub struct AsyncConnection {
    client: Client,
}

/// A `bb8::ManageConnection` for `tokio_postgres::Connection`s.
pub struct PostgresConnectionManager {
    params: ConnectParams,
    tls_mode: Box<Fn() -> TlsMode + Send + Sync>,
}

impl PostgresConnectionManager {
    /// Create a new `PostgresConnectionManager`.
    pub fn new<F>(params: ConnectParams, tls_mode: F) -> Result<PostgresConnectionManager>
    where
        F: Fn() -> TlsMode + Send + Sync + 'static,
    {
        Ok(PostgresConnectionManager {
            // TODO: remove this unwrap, there used to be a type this matched in tokio pg
            params: params,
            tls_mode: Box::new(tls_mode),
        })
    }
}

impl l3_37::ManageConnection for PostgresConnectionManager {
    type Connection = AsyncConnection;
    type Error = Error;

    fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'static> {
        use tokio::executor::spawn;
        Box::new(
            tokio_postgres::connect(self.params.clone(), (self.tls_mode)()).map(
                |(client, connection)| {
                    spawn(connection.map_err(|e| panic!("{}", e)));
                    AsyncConnection { client: client }
                },
            ),
        )
    }

    fn is_valid(&self, mut conn: Self::Connection) -> Box<Future<Item = (), Error = Self::Error>> {
        // If we can execute this without erroring, we're definitely still connected to the datbase
        Box::new(conn.client.batch_execute(""))
    }

    // fn has_broken(&self, conn: &mut Self::Connection) -> bool {
    //     false
    //     // this method was removed with no replacement?
    //     // conn.is_desynchronized()
    // }

    fn timed_out(&self) -> Self::Error {
        unimplemented!()
        // Error::io(io::ErrorKind::TimedOut.into())
    }
}

impl fmt::Debug for PostgresConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
            .field("params", &self.params)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Stream;
    use l3_37::{Config, Pool};
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::runtime::current_thread::Runtime;
    use tokio_postgres::params::IntoConnectParams;

    #[test]
    fn it_works() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .into_connect_params()
                .unwrap(),
            || tokio_postgres::TlsMode::None,
        ).unwrap();

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
                    }).map(|connection| ((), connection))
            })
        });

        runtime.block_on(future).expect("could not run");
    }

    #[test]
    fn it_allows_multiple_queries_at_the_same_time() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .into_connect_params()
                .unwrap(),
            || tokio_postgres::TlsMode::None,
        ).unwrap();

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
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
            });

            let q2 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 2::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(2, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
            });

            q1.join(q2)
        });

        runtime.block_on(future).expect("could not run");
    }

    #[test]
    fn it_reuses_connections() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .into_connect_params()
                .unwrap(),
            || tokio_postgres::TlsMode::None,
        ).unwrap();

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
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
            });

            let q2 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 2::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(2, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
            });

            let q3 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 3::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(3, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    })
            });

            q1.join3(q2, q3)
        });

        runtime.block_on(future).expect("could not run");
    }
}
