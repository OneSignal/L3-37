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

mod config;
mod conn;
mod error;
mod inner;
mod manage_connection;
mod queue;

use futures::channel::oneshot;
use futures::future::{self};
use futures::prelude::*;
use futures::stream;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::error::InternalError;

pub use crate::config::Config;
pub use conn::Conn;
pub use error::Error;
pub use manage_connection::ManageConnection;

use inner::ConnectionPool;
use queue::{Live, Queue};

/// General connection pool
pub struct Pool<C: ManageConnection + Send> {
    conn_pool: Arc<ConnectionPool<C>>,
    config: Arc<Config>,
}

impl<C: ManageConnection + Send + fmt::Debug> fmt::Debug for Pool<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Pool")
            .field("conn_pool", &self.conn_pool)
            .finish()
    }
}

impl<C: ManageConnection> Pool<C> {
    /// Minimum number of connections in the pool.
    pub fn min_conns(&self) -> usize {
        self.config.min_size
    }

    /// Maximum possible number of connections in the pool.
    pub fn max_conns(&self) -> usize {
        self.config.max_size
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
            config: self.config.clone(),
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
            .try_fold(Queue::new(config.idle_queue_size), |conns, conn| {
                conns.new_conn(Live::new(conn));

                future::ok(conns)
            })
            .await?;

        let config = Arc::new(config);

        // Set up the pool once the connections are established
        let conn_pool = Arc::new(ConnectionPool::new(conns, manager, Arc::clone(&config)));

        Ok(Pool { conn_pool, config })
    }

    /// Returns a future that resolves to a connection from the pool.
    ///
    /// If there are connections that are available to be used, the future will resolve immediately,
    /// otherwise, the connection will be in a pending state until a future is returned to the pool.
    ///
    /// Timeout ability can be added to this method by calling `connection_timeout` on the `Config`.
    pub async fn connection(&self) -> Result<Conn<C>, Error<C::Error>> {
        if let Some(timeout) = self.config.connect_timeout {
            tokio::time::timeout(timeout, self.connect_no_timeout()).await?
        } else {
            self.connect_no_timeout().await
        }
    }

    async fn connect_no_timeout(&self) -> Result<Conn<C>, Error<C::Error>> {
        if !self.config.test_on_check_out {
            return self.try_get_connection().await;
        }

        // We go through the max size loop here to:
        //
        // 1) Get a new connection.
        // 2) Grab a connection from the pool.
        //
        // In case 1, it will wait in the future and either get the
        // new connection or return error.  In case 2, we check the validity
        // of the connection and remove the invalid connection from the pool
        // and re-create a new connection, as there will be a new slot for
        // the connection in the pool.  In case of the network disconnect,
        // it usually hit the error in the second try and returns error, as
        // in the case 1 above.  Hence, we usually won't try the case 2 for
        // `max_size` count.
        for _ in 0..self.conn_pool.max_size() {
            let mut connection = self.try_get_connection().await?;

            match self.conn_pool.is_valid(&mut connection).await {
                Ok(()) => return Ok(connection),
                Err(e) => {
                    debug!(
                        "connection: found connection in pool that is no longer valid - removing from pool: {:?}",
                        e
                    );

                    connection.forget();

                    self.conn_pool.conns.decrement();
                    debug!(
                        "connection count is now: {:?}",
                        self.conn_pool.conns.total()
                    );
                    self.spawn_new_future_loop();

                    // In my experience, unconstrained loops in async fns are
                    // problematic because tokio's scheduler will allow the task
                    // to block up the runtime, so it is useful to add some
                    // delays to loops to allow other futures to be polled.
                    tokio::time::delay_for(Duration::from_millis(100)).await;
                }
            }
        }

        Err(Error::Internal(InternalError::AllConnectionsInvalid))
    }

    async fn try_get_connection(&self) -> Result<Conn<C>, Error<C::Error>> {
        {
            if let Some(conn) = self.conn_pool.conns.get() {
                debug!("connection: connection already in pool and ready to go");
                return Ok(Conn::new(conn, self.clone()));
            } else {
                debug!("connection: try spawn connection");
                if let Some(conn) = self.try_spawn_connection().await {
                    let conn = conn?;
                    let this = self.clone();
                    debug!("connection: spawned connection");

                    return Ok(Conn::new(conn, this));
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
        Ok(Conn::new(conn, this))
    }

    /// Attempt to spawn a new connection. If we're not already over the max number of connections,
    /// a future will be returned that resolves to the new connection.
    /// Otherwise, None will be returned
    async fn try_spawn_connection(&self) -> Option<Result<Live<C::Connection>, Error<C::Error>>> {
        match self
            .conn_pool
            .conns
            .safe_increment(self.conn_pool.max_size())
        {
            Some(_) => {
                debug!("try_spawn_connection: starting connection");
                let result = match self.conn_pool.connect().await {
                    Ok(conn) => Ok(Live::new(conn)),
                    Err(err) => {
                        // if we weren't able to make a new connection, we need to decrement
                        // connections, since we preincremented the connection count for this one.
                        self.conn_pool.conns.decrement();
                        Err(err)
                    }
                };
                Some(result)
            }
            None => None,
        }
    }

    /// Receive a connection back to be stored in the pool. This could have one
    /// of two outcomes:
    /// * The connection will be passed to a waiting future, if any exist.
    /// * The connection will be put back into the connection pool.
    pub fn put_back(&self, mut conn: Live<C::Connection>) {
        debug!("put_back: start put back");

        let broken = self.conn_pool.has_broken(&mut conn);
        if broken {
            self.conn_pool.conns.decrement();
            debug!(
                "connection count is now: {:?}",
                self.conn_pool.conns.total()
            );
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
        if self.conn_pool.conns.store(conn).is_err() {
            debug!("put_back: hit the idle connection queue limit");
        }
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
                        debug!("creating new connection from spawn loop");
                        this.conn_pool.conns.increment();

                        this.put_back(Live::new(conn));

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
    pub fn total_conns(&self) -> usize {
        self.conn_pool.conns.total()
    }

    /// The number of idle connections in the pool.
    pub fn idle_conns(&self) -> usize {
        self.conn_pool.conns.idle()
    }

    /// The number of errors when the connection push back to the pool.
    pub fn idle_conns_push_error(&self) -> usize {
        self.conn_pool.conns.idle_push_error_count()
    }

    /// The number of waiters for the next available connections.
    pub fn waiters(&self) -> usize {
        self.conn_pool.waiting.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::*, Arc};
    use std::time::Duration;
    use tokio::time::timeout;

    #[derive(Debug)]
    pub struct DummyManager {
        last_id: AtomicUsize,
    }

    #[derive(Debug)]
    pub struct DummyValue {
        id: usize,
        valid: Arc<AtomicBool>,
        broken: bool,
    }

    #[derive(Debug, Fail)]
    #[fail(display = "DummyError")]
    pub struct DummyError;

    impl DummyManager {
        pub fn new() -> Self {
            Self {
                last_id: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ManageConnection for DummyManager {
        type Connection = DummyValue;
        type Error = DummyError;

        async fn connect(&self) -> Result<Self::Connection, Error<Self::Error>> {
            Ok(DummyValue {
                id: self.last_id.fetch_add(1, Ordering::SeqCst),
                valid: Arc::new(AtomicBool::new(true)),
                broken: false,
            })
        }

        async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Error<Self::Error>> {
            if conn.valid.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(Error::External(DummyError))
            }
        }

        fn has_broken(&self, conn: &mut Self::Connection) -> bool {
            conn.broken
        }

        /// Produce an error representing a connection timeout.
        fn timed_out(&self) -> Error<Self::Error> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_min_max_conns() {
        let mngr = DummyManager::new();

        let config = Config::new().min_size(1).max_size(2);
        let pool = Arc::new(Pool::new(mngr, config).await.unwrap());

        assert_eq!(pool.min_conns(), 1);
        assert_eq!(pool.max_conns(), 2);
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
        let config: Config = Config::new().min_size(1).max_size(1);

        // pool is of size , we try to get 2 connections so the second one will never resolve
        let pool = Pool::new(mngr, config).await.unwrap();
        // Forget the values so we don't drop them, and return them back to the pool
        ::std::mem::forget(pool.connection().await.unwrap());

        let result = tokio::time::timeout(Duration::from_millis(10), pool.connection()).await;

        assert!(result.is_err(), "didn't timeout");
    }

    #[tokio::test]
    async fn it_times_out_when_no_connections_available() {
        let mngr = DummyManager::new();

        let config = Config::new()
            .max_size(1)
            .connection_timeout(Duration::from_millis(100));

        let pool = Pool::new(mngr, config).await.unwrap();
        let conn1 = pool.connection().await.unwrap();

        let result = pool.connection().await;

        match result {
            Err(Error::Internal(InternalError::TimedOut)) => {}
            _ => panic!("connection should timeout"),
        }

        drop(conn1);
    }

    #[tokio::test]
    async fn it_allocates_new_connections_up_to_max_size() {
        let mngr = DummyManager::new();
        let config: Config = Config::new().min_size(1).max_size(2);

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
    async fn it_does_not_return_connections_that_are_invalid() {
        let mngr = DummyManager::new();
        let config: Config = Config::new().max_size(2).min_size(1);

        let pool = Pool::new(mngr, config).await.unwrap();

        let conn1 = pool.connection().await.unwrap();

        let conn1_id = conn1.id;
        let conn1_valid = Arc::clone(&conn1.valid);

        // return conn1 to the connection pool
        drop(conn1);

        let conn1 = pool.connection().await.unwrap();

        // The connection is still valid, so this should be the same as the
        // original connection.
        assert_eq!(conn1.id, conn1_id);

        // return conn1 to the pool
        drop(conn1);

        // mark the connection as invalid
        conn1_valid.store(false, Ordering::SeqCst);

        // we should notice that the connection is invalid and create a new one
        // before returning it
        let conn2 = pool.connection().await.unwrap();

        assert_ne!(
            conn2.id, conn1_id,
            "Conn1 was returned from the pool even though it is marked as invalid"
        );
    }

    #[tokio::test]
    async fn it_does_return_connections_that_are_invalid_if_so_configured() {
        let mngr = DummyManager::new();
        let config: Config = Config::new()
            .max_size(2)
            .min_size(1)
            .test_on_check_out(false);

        let pool = Pool::new(mngr, config).await.unwrap();

        let conn1 = pool.connection().await.unwrap();

        let conn1_id = conn1.id;
        let conn1_valid = Arc::clone(&conn1.valid);

        // return conn1 to the connection pool
        drop(conn1);

        let conn1 = pool.connection().await.unwrap();

        // The connection is still valid, so this should be the same as the
        // original connection.
        assert_eq!(conn1.id, conn1_id);

        // return conn1 to the pool
        drop(conn1);

        // mark the connection as invalid
        conn1_valid.store(false, Ordering::SeqCst);

        // we should notice that the connection is invalid and create a new one
        // before returning it
        let conn1 = pool.connection().await.unwrap();

        assert_eq!(
            conn1.id, conn1_id,
            "Conn1 was not returned from the pool even though it is marked as invalid, and the pool should not check validity"
        );
    }

    #[tokio::test]
    async fn it_does_not_return_connections_that_are_broken() {
        let mngr = DummyManager::new();
        let config: Config = Config::new().max_size(2).min_size(1);

        let pool = Pool::new(mngr, config).await.unwrap();

        let conn1 = pool.connection().await.unwrap();

        let conn1_id = conn1.id;

        // return conn1 to the connection pool
        drop(conn1);

        let mut conn1 = pool.connection().await.unwrap();

        // The connection is still valid, so this should be the same as the
        // original connection.
        assert_eq!(conn1.id, conn1_id);

        // mark conn1 as broken, it should not be returned to the pool.
        conn1.broken = true;

        // try to return conn1 to the pool - should not be returned because it
        // is marked as broken, and a new connection should be spawned in the
        // background.
        drop(conn1);

        let conn2 = pool.connection().await.unwrap();

        assert_ne!(
            conn2.id, conn1_id,
            "Conn1 was returned from the pool even though it is marked as broken"
        );
    }

    #[tokio::test]
    async fn test_can_be_accessed_by_mutliple_futures_concurrently() {
        let mngr = DummyManager::new();

        let config = Config::new().min_size(2).max_size(2);
        let pool = Arc::new(Pool::new(mngr, config).await.unwrap());
        let count = Arc::new(AtomicUsize::new(0));

        futures::join!(
            loop_run(Arc::clone(&count), Arc::clone(&pool)),
            loop_run(Arc::clone(&count), Arc::clone(&pool))
        );

        assert_eq!(pool.total_conns(), 2);
        assert_eq!(pool.idle_conns(), 2);
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
