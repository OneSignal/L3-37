// From c3po, https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/lib.rs#L33
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

use futures::future::{self, Future};
use std::ops::{Deref, DerefMut};

use manage_connection::ManageConnection;
use queue::Live;
use Pool;

/// Connection future
pub type ConnFuture<T, E> =
    future::Either<future::FutureResult<T, E>, Box<dyn Future<Item = T, Error = E> + Send>>;

// From c3po, https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/lib.rs#L40

/// A smart wrapper around a connection which stores it back in the pool
/// when it is dropped.
///
/// This can be dereferences to the `Service` instance this pool manages, and
/// also implements `Service` itself by delegating.
pub struct Conn<C: ManageConnection> {
    /// Actual connection. This should never become a None variant under normal operation.
    /// This is an option so we can take the connection on drop, and push it back into the pool
    pub conn: Option<Live<C::Connection>>,
    /// Underlying pool. A reference is stored here so we can push the connection back into the
    /// pool on drop
    pub pool: Pool<C>,
}

impl<C: ManageConnection> Deref for Conn<C> {
    type Target = C::Connection;
    fn deref(&self) -> &Self::Target {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<C: ManageConnection> DerefMut for Conn<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<C: ManageConnection> Drop for Conn<C> {
    fn drop(&mut self) {
        let conn = self.conn.take().unwrap();
        self.pool.put_back(conn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tests::DummyManager;
    use tokio::runtime::current_thread::Runtime;
    use Config;
    use Pool;

    #[test]
    fn conn_pushes_back_into_pool_after_drop() {
        let mngr = DummyManager {};
        let config = Config {
            min_size: 2,
            max_size: 2,
        };

        let future = Pool::new(mngr, config).and_then(|pool| {
            assert_eq!(pool.idle_conns(), 2);

            pool.connection().and_then(move |conn| {
                assert_eq!(pool.idle_conns(), 1);

                ::std::mem::drop(conn);

                assert_eq!(pool.idle_conns(), 2);
                Ok(())
            })
        });

        Runtime::new()
            .expect("could not run")
            .block_on(future)
            .expect("could not run");
    }
}
