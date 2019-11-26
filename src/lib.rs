#![deny(missing_docs)]
// Copyright (c) 2014 The Rust Project Developers

// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:

// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

// Copyright 2014  The Rust Project Developers

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// 	http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Connection pooling library for tokio.
//!
//! Any connection type that implements the `ManageConnection` trait can be used with this libary.

mod conn;
mod error;
mod inner;
mod manage_connection;
mod queue;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use log::{debug, error};
use std::iter::Iterator;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::executor;
use tokio::sync::oneshot;
use tokio::timer;

pub use conn::{Conn, ConnFuture};
pub use manage_connection::ManageConnection;

use inner::ConnectionPool;
use queue::{Live, Queue};

/// General connection pool
pub struct Pool<C: ManageConnection + Send> {
    conn_pool: Arc<ConnectionPool<C>>,
}

/// Configuration for the connection pool
#[derive(Debug)]
pub struct Config {
    /// Minimum number of connections in the pool. The pool will be initialied with this number of
    /// connections
    pub min_size: usize,
    /// Max number of connections to keep in the pool
    pub max_size: usize,
}

/// Error type returned by this module
#[derive(Debug)]
pub enum Error<E: Send + 'static> {
    /// Error coming from the connection pooling itself
    Internal(error::InternalError),
    /// Error from the connection manager or the underlying client
    External(E),
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_size: 10,
            min_size: 1,
        }
    }
}

/// Returns a new `Pool` referencing the same state as `self`.
impl<C> Clone for Pool<C>
where
    C: ManageConnection,
{
    fn clone(&self) -> Pool<C> {
        Pool {
            conn_pool: self.conn_pool.clone(),
        }
    }
}

impl<C: ManageConnection + Send> Pool<C> {
    /// Creates a new connection pool
    ///
    /// The returned future will resolve to the pool if successful, which can then be used
    /// immediately.
    pub async fn new(manager: C, config: Config) -> Result<Pool<C>, Error<C::Error>> {
        assert!(
            config.max_size >= config.min_size,
            "max_size of pool must be greater than or equal to the min_size"
        );

        let conns: stream::futures_unordered::FuturesUnordered<_> = std::iter::repeat(&manager)
            .take(config.min_size)
            .map(|c| c.connect())
            .collect();
        let conns = conns.collect::<Vec<_>>().await;
        let conns: Result<Vec<_>, _> = conns.into_iter().collect();
        let conns = conns?.into_iter().fold(Queue::new(), |conns, conn| {
            conns.new_conn(Live::new(conn));
            conns
        });

        let conn_pool = Arc::new(ConnectionPool::new(conns, manager, config));
        Ok(Pool { conn_pool })
    }

    /// Returns a future that resolves to a connection from the pool.
    ///
    /// If there are connections that are available to be used, the future will resolve immediately,
    /// otherwise, the connection will be in a pending state until a future is returned to the pool.
    ///
    /// This **does not** implement any timeout functionality. Timeout functionality can be added
    /// by calling `.timeout` on the returned future.
    pub async fn connection(&self) -> Result<Conn<C>, Error<C::Error>> {
        let conns = self
            .conn_pool
            .conns
            .lock()
            .expect("posioned connection mutex");
        /*
        debug!("connection: acquired connection lock");
        if let Some(conn) = conns.get() {
            debug!("connection: connection already in pool and ready to go");
            Ok(Conn {
                conn: Some(conn),
                pool: self.clone()
            })
        } else {
            debug!("connection: try spawn connection");
            if let Some(conn_future) = Self::try_spawn_connection(&self, &conns) {
                let this = self.clone();
                debug!("connection: try spawn connection");
                /*return future::Either::B(Box::new(conn_future.map(|conn| Conn {
                    conn: Some(conn),
                    pool: this,
                })));*/
            }
            // Have the pool notify us of the connection
            let (tx, rx) = oneshot::channel();
            debug!("connection: pushing to notify of connection");
            self.conn_pool.notify_of_connection(tx);

            // Prepare the future which will wait for a free connection
            let this = self.clone();
            debug!("connection: waiting for connection");
            future::Either::B(Box::new(
                rx.map(|conn| {
                    debug!("connection: got connection after waiting");
                    Conn {
                        conn: Some(conn),
                        pool: this,
                    }
                })
                .map_err(|_err| unimplemented!()),
            ))
        }*/
        let conn = match conns.get() {
            Some(conn) => {
                println!("connection: connection already in pool and ready to go");
                Ok(conn)
            }
            None => {
                println!("connection: try spawn connection");
                match Self::try_spawn_connection(&self, &conns).await {
                    Some(result) => result,
                    None => {
                        let (tx, rx) = oneshot::channel();
                        debug!("connection: pushing to notify of connection");
                        self.conn_pool.notify_of_connection(tx);
                        match rx.await {
                            Ok(conn) => Ok(conn),
                            Err(_) => unimplemented!(),
                        }
                    }
                }
            }
        }?;
        Ok(Conn {
            conn: Some(conn),
            pool: self.clone(),
        })
    }
    /// Attempt to spawn a new connection. If we're not already over the max number of connections,
    /// a future will be returned that resolves to the new connection.
    /// Otherwise, None will be returned
    pub(crate) async fn try_spawn_connection(
        this: &Self,
        conns: &Arc<queue::Queue<<C as ManageConnection>::Connection>>,
    ) -> Option<Result<Live<C::Connection>, Error<C::Error>>> {
        if let Some(_) = conns.safe_increment(this.conn_pool.max_size()) {
            let result = this.conn_pool.connect().await;
            Some(match result {
                Ok(conn) => Ok(Live::new(conn)),
                Err(err) => {
                    // if we weren't able to make a new connection, we need to decrement
                    // connections, since we preincremented the connection count for this  one
                    conns.decrement();
                    Err(err)
                }
            })
        } else {
            None
        }
    }
    /// Receive a connection back to be stored in the pool. This could have one
    /// of two outcomes:
    /// * The connection will be passed to a waiting future, if any exist.
    /// * The connection will be put back into the connection pool.
    pub fn put_back(&self, mut conn: Live<C::Connection>) {
        debug!("put_back: start put back");

        let broken = self.conn_pool.has_broken(&mut conn);
        let conns = self
            .conn_pool
            .conns
            .lock()
            .expect("posioned connection mutex");
        debug!("put_back: got lock for put back");

        if broken {
            conns.decrement();
            debug!("connection count is now: {:?}", conns.total());
            self.spawn_new_future_loop();
            return;
        }

        // first attempt to send it to any waiting requests
        let mut conn = conn;
        while let Some(waiting) = self.conn_pool.try_waiting() {
            debug!("put_back: got a waiting connection, sending");
            conn = match waiting.send(conn) {
                Ok(_) => return,
                Err(conn) => {
                    debug!("put_back: unable to send connection");
                    conn
                }
            };
        }
        debug!("put_back: no waiting connection, storing");

        // If there are no waiting requests & we aren't over the max idle
        // connections limit, attempt to store it back in the pool
        conns.store(conn);
    }

    fn spawn_new_future_loop(&self) {
        let this1 = self.clone();
        /*executor::spawn(future::loop_fn((), move |_| {
            let this = this1.clone();
            this.conn_pool.connect().then(move |res| match res {
                Ok(conn) => {
                    // Call put_back instead of new_conn because we want to give the waiting futures
                    // a chance to get the connection if there are any.
                    // However, this means we have to call increment before calling put_back,
                    // as put_back assumes that the connection already exists.
                    // This could probably use some refactoring
                    let conns = this
                        .conn_pool
                        .conns
                        .lock()
                        .expect("posioned connection mutex");
                    debug!("creating new connection from spawn loop");
                    conns.increment();
                    // Drop so we free the lock
                    ::std::mem::drop(conns);

                    this.put_back(Live::new(conn));

                    Either::A(future::ok(future::Loop::Break(())))
                }
                Err(err) => {
                    error!(
                        "unable to establish new connection, trying again: {:?}",
                        err
                    );
                    // TODO: make this use config
                    Either::B(
                        Delay::new(Instant::now() + Duration::from_secs(1))
                            .map(|_| future::Loop::Continue(()))
                            .map_err(|_e| panic!("delay timer errored, shutdown required")),
                    )
                }
            })
        }));*/
        executor::spawn(async move {
            loop {
                let this = this1.clone();
                match this.conn_pool.connect().await {
                    Ok(conn) => {
                        let conns = this
                            .conn_pool
                            .conns
                            .lock()
                            .expect("posioned connection mutex");
                        debug!("creating new connection from spawn loop");
                        conns.increment();
                        ::std::mem::drop(conns);
                        this.put_back(Live::new(conn));
                        break;
                    }
                    Err(err) => {
                        error!(
                            "unable to establish new connection, trying again: {:?}",
                            err
                        );
                        timer::delay(Instant::now() + Duration::from_secs(1)).await;
                    }
                }
                //let result = this.conn_pool.connect().await;
            }
        });
    }

    /// The total number of connections in the pool.
    pub fn total_conns(&self) -> usize {
        let conns = self
            .conn_pool
            .conns
            .lock()
            .expect("posioned connection mutex");
        conns.total()
    }

    /// The number of idle connections in the pool.
    pub fn idle_conns(&self) -> usize {
        let conns = self
            .conn_pool
            .conns
            .lock()
            .expect("posioned connection mutex");
        conns.idle()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::FutureExt;
    use futures::join;
    use std::time::Duration;
    use tokio::future::FutureExt as TokioFutureExt;
    use tokio::runtime::current_thread::Runtime;

    #[derive(Debug)]
    pub struct DummyManager {}
    #[async_trait]
    impl ManageConnection for DummyManager {
        type Connection = ();
        type Error = ();

        async fn connect(&self) -> Result<Self::Connection, Error<Self::Error>> {
            Ok(())
        }

        async fn is_valid(&self, _conn: Self::Connection) -> Result<(), Error<Self::Error>> {
            unimplemented!()
        }

        fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
            false
        }

        /// Produce an error representing a connection timeout.
        fn timed_out(&self) -> Error<Self::Error> {
            unimplemented!()
        }
    }

    #[test]
    fn simple_pool_creation_and_connection() {
        let mngr = DummyManager {};
        let config: Config = Default::default();
        Runtime::new().expect("could not run").block_on(async {
            let pool = Pool::new(mngr, config).await.unwrap();
            let conn = pool.connection().await.unwrap();
            if let Some(Live {
                conn: (),
                live_since: _,
            }) = conn.conn
            {
                ()
            } else {
                panic!("connection is not correct type")
            }
        });
    }

    #[test]
    fn it_returns_a_non_resolved_future_when_over_pool_limit() {
        let mngr = DummyManager {};
        let config: Config = Config {
            max_size: 1,
            min_size: 1,
        };

        Runtime::new().expect("could not run").block_on(async {
            let pool = Pool::new(mngr, config).await.unwrap();
            ::std::mem::forget(pool.connection().await);
            let result = pool.connection().timeout(Duration::from_millis(10)).await;
            match result {
                Ok(_) => panic!("didn't timeout"),
                Err(err) => (),
            }
        });
    }

    #[test]
    fn it_allocates_new_connections_up_to_max_size() {
        let mngr = DummyManager {};
        let config: Config = Config {
            max_size: 2,
            min_size: 1,
        };

        // pool is of size 1, but is allowed to generate new connections up to 2.
        // When we try 2 connections, they should both pass without timing out
        Runtime::new().expect("could not run").block_on(async {
            let pool = Pool::new(mngr, config).await.unwrap();
            let f1 = pool
                .connection()
                .timeout(Duration::from_millis(10))
                .map(|res| match res {
                    Ok(conn) => {
                        ::std::mem::forget(conn);
                        ()
                    }
                    Err(_) => panic!("second connection timed out"),
                });
            let f2 = pool
                .connection()
                .timeout(Duration::from_millis(10))
                .map(|res| match res {
                    Ok(conn) => {
                        ::std::mem::forget(conn);
                        ()
                    }
                    Err(_) => panic!("second connection timed out"),
                });
            join!(f1, f2);
        });
    }
}
