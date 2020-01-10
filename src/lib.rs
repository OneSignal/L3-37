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

extern crate crossbeam_queue;
extern crate futures;
extern crate tokio;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;

mod conn;
mod error;
mod inner;
mod manage_connection;
mod queue;

use futures::channel::oneshot;
use futures::future::{self};
use futures::prelude::*;
use futures::stream;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::error::InternalError;

pub use conn::Conn;
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

        let conns: stream::FuturesUnordered<_> = std::iter::repeat(&manager)
            .take(config.min_size)
            .map(|c| c.connect())
            .collect();

        // Fold the connections we are creating into a Queue object
        let conns = conns
            .try_fold(Queue::new(), |conns, conn| {
                conns.new_conn(Live::new(conn));

                future::ok(conns)
            })
            .await?;

        // Set up the pool once the connections are established
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
        {
            let conns = self.conn_pool.conns.lock().await;

            debug!("connection: acquired connection lock");
            if let Some(conn) = conns.get() {
                debug!("connection: connection already in pool and ready to go");
                return Ok(Conn {
                    conn: Some(conn),
                    pool: Some(self.clone()),
                });
            } else {
                debug!("connection: try spawn connection");
                if let Some(conn) = Self::try_spawn_connection(&self, &conns).await {
                    let conn = conn?;
                    let this = self.clone();
                    debug!("connection: try spawn connection");

                    return Ok(Conn {
                        conn: Some(conn),
                        pool: Some(this),
                    });
                }
            }
        }

        // Have the pool notify us of the connection
        let (tx, rx) = oneshot::channel();
        debug!("connection: pushing to notify of connection");
        self.conn_pool.notify_of_connection(tx);

        // Prepare the future which will wait for a free connection
        let this = self.clone();
        debug!("connection: waiting for connection");

        let conn = rx.await.map_err(|_| {
            Error::Internal(InternalError::Other(
                "Connection channel was closed unexpectedly".into(),
            ))
        })?;

        debug!("connection: got connection after waiting");
        Ok(Conn {
            conn: Some(conn),
            pool: Some(this),
        })
    }

    /// Attempt to spawn a new connection. If we're not already over the max number of connections,
    /// a future will be returned that resolves to the new connection.
    /// Otherwise, None will be returned
    pub(crate) async fn try_spawn_connection(
        this: &Self,
        conns: &Arc<queue::Queue<<C as ManageConnection>::Connection>>,
    ) -> Option<Result<Live<C::Connection>, Error<C::Error>>> {
        if conns.safe_increment(this.conn_pool.max_size()).is_some() {
            let conns = Arc::clone(&conns);
            let result = match this.conn_pool.connect().await {
                Ok(conn) => Ok(Live::new(conn)),
                Err(err) => {
                    // if we weren't able to make a new connection, we need to decrement
                    // connections, since we preincremented the connection count for this  one
                    conns.decrement();
                    Err(err)
                }
            };

            Some(result)
        } else {
            None
        }
    }
    /// Receive a connection back to be stored in the pool. This could have one
    /// of two outcomes:
    /// * The connection will be passed to a waiting future, if any exist.
    /// * The connection will be put back into the connection pool.
    pub async fn put_back(&self, mut conn: Live<C::Connection>) {
        debug!("put_back: start put back");

        let broken = self.conn_pool.has_broken(&mut conn);
        let conns = self.conn_pool.conns.lock().await;
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
        tokio::spawn(async move {
            loop {
                let this = this1.clone();

                let res = this.conn_pool.connect().await;
                match res {
                    Ok(conn) => {
                        // Call put_back instead of new_conn because we want to give the waiting futures
                        // a chance to get the connection if there are any.
                        // However, this means we have to call increment before calling put_back,
                        // as put_back assumes that the connection already exists.
                        // This could probably use some refactoring
                        let conns = this.conn_pool.conns.lock().await;
                        debug!("creating new connection from spawn loop");
                        conns.increment();
                        // Drop so we free the lock
                        ::std::mem::drop(conns);

                        this.put_back(Live::new(conn)).await;

                        break;
                    }
                    Err(err) => {
                        error!(
                            "unable to establish new connection, trying again: {:?}",
                            err
                        );
                        // TODO: make this use config
                        time::delay_for(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    /// The total number of connections in the pool.
    pub async fn total_conns(&self) -> usize {
        let conns = self.conn_pool.conns.lock().await;
        conns.total()
    }

    /// The number of idle connections in the pool.
    pub async fn idle_conns(&self) -> usize {
        let conns = self.conn_pool.conns.lock().await;
        conns.idle()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::*, Arc};
    use std::time::Duration;
    use tokio::time::timeout;

    #[derive(Debug)]
    pub struct DummyManager {}

    impl DummyManager {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl ManageConnection for DummyManager {
        type Connection = ();
        type Error = ();

        async fn connect(&self) -> Result<Self::Connection, Error<Self::Error>> {
            Ok(())
        }

        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Error<Self::Error>> {
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

    #[tokio::test]
    async fn simple_pool_creation_and_connection() {
        let mngr = DummyManager::new();
        let config: Config = Default::default();

        let pool = Pool::new(mngr, config).await.unwrap();
        let conn = pool.connection().await.unwrap();

        assert!(conn.conn.is_some(), "connection is not correct type");
    }

    #[tokio::test]
    async fn it_returns_a_non_resolved_future_when_over_pool_limit() {
        let mngr = DummyManager::new();
        let config: Config = Config {
            max_size: 1,
            min_size: 1,
        };

        // pool is of size , we try to get 2 connections so the second one will never resolve
        let pool = Pool::new(mngr, config).await.unwrap();
        // Forget the values so we don't drop them, and return them back to the pool
        ::std::mem::forget(pool.connection().await.unwrap());

        let result = tokio::time::timeout(Duration::from_millis(10), pool.connection()).await;

        assert!(result.is_err(), "didn't timeout");
    }

    #[tokio::test]
    async fn it_allocates_new_connections_up_to_max_size() {
        let mngr = DummyManager::new();
        let config: Config = Config {
            max_size: 2,
            min_size: 1,
        };

        // pool is of size 1, but is allowed to generate new connections up to 2.
        // When we try 2 connections, they should both pass without timing out
        let pool = Pool::new(mngr, config).await.unwrap();

        let connection = pool.connection().await.unwrap();

        // Forget the values so we don't drop them, and return them back to the pool
        ::std::mem::forget(connection);

        let f1 = async {
            let conn = tokio::time::timeout(Duration::from_millis(10), pool.connection())
                .await
                .expect("second connection timed out");
            ::std::mem::forget(conn);
        };

        // The third connection should timeout though, as we're only allowed to go up to 2
        let f2 = async {
            let result = tokio::time::timeout(Duration::from_millis(10), pool.connection()).await;

            assert!(result.is_err(), "third didn't timeout");
        };

        futures::join!(f1, f2);
    }

    #[tokio::test]
    async fn test_can_be_accessed_by_mutliple_futures_concurrently() {
        let mngr = DummyManager::new();

        let config = Config {
            min_size: 2,
            max_size: 2,
        };
        let pool = Arc::new(Pool::new(mngr, config).await.unwrap());
        let count = Arc::new(AtomicUsize::new(0));

        futures::join!(
            loop_run(Arc::clone(&count), Arc::clone(&pool)),
            loop_run(Arc::clone(&count), Arc::clone(&pool))
        );

        assert_eq!(pool.total_conns().await, 2);
        assert_eq!(pool.idle_conns().await, 2);
    }

    async fn loop_run(count: Arc<AtomicUsize>, pool: Arc<Pool<DummyManager>>) {
        tokio::spawn(async move {
            loop {
                timeout(Duration::from_secs(5), pool.connection())
                    .await
                    .expect("connection timed out")
                    .expect("error getting connection");
                let old_count = count.fetch_add(1, Ordering::SeqCst);
                if old_count + 1 >= 100 {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }
}
