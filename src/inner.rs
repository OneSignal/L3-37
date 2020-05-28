// Most of this comes from c3po's inner module: https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/inner.rs
// with some additions and updates to work with modern versions of tokio
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

use crossbeam_queue::SegQueue;
use futures::channel::oneshot;
use std::fmt;
use std::sync::Arc;

use crate::manage_connection::ManageConnection;
use crate::queue::{Live, Queue};
use crate::Config;
use crate::Error;

/// Inner connection pool. Handles creating and holding the connections, as well as keeping track of
/// futures that are waiting on connections.
pub struct ConnectionPool<C: ManageConnection + Send> {
    /// Queue of connections in the pool
    pub(crate) conns: Arc<Queue<C::Connection>>,
    /// Queue of oneshot's that are waiting to be given a new connection when the current pool is
    /// already saturated.
    pub(crate) waiting: SegQueue<oneshot::Sender<Live<C::Connection>>>,
    /// Connection manager used to create new connections as needed
    manager: C,
    /// Configuration for the pool
    config: Arc<Config>,
}

impl<C: ManageConnection + Send + fmt::Debug> fmt::Debug for ConnectionPool<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("waiting_count", &self.waiting.len())
            .field("manager", &self.manager)
            .field("config", &self.config)
            .finish()
    }
}

impl<C: ManageConnection> ConnectionPool<C> {
    /// Creates a new connection pool
    pub fn new(conns: Queue<C::Connection>, manager: C, config: Arc<Config>) -> ConnectionPool<C> {
        ConnectionPool {
            conns: Arc::new(conns),
            waiting: SegQueue::new(),
            manager,
            config,
        }
    }

    pub fn max_size(&self) -> usize {
        self.config.max_size
    }

    pub async fn connect(&self) -> Result<C::Connection, Error<C::Error>> {
        self.manager.connect().await
    }

    /// Adds a "waiter" to the queue of waiting futures. When a new connection becomes available,
    /// the oneshot will be called with a new connection
    pub fn notify_of_connection(&self, tx: oneshot::Sender<Live<C::Connection>>) {
        self.waiting.push(tx);
    }

    pub fn try_waiting(
        &self,
    ) -> Option<oneshot::Sender<Live<<C as ManageConnection>::Connection>>> {
        self.waiting.pop().ok()
    }

    pub async fn is_valid(&self, conn: &mut C::Connection) -> Result<(), Error<C::Error>> {
        self.manager.is_valid(conn).await
    }

    pub fn has_broken(&self, conn: &mut Live<C::Connection>) -> bool {
        self.manager.has_broken(&mut conn.conn)
    }
}
