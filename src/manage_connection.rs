// Most of this trait comes from https://github.com/khuey/bb8/blob/9f7d763b55211001d8168ebfde449d8b0d0614f6/src/lib.rs#L42,
// with some minor changes to update it for tokio 0.1.7
//The MIT License (MIT)

// Copyright (c) 2018 Kyle Huey

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use crate::Error as L337Error;

/// A trait which provides connection-specific functionality.
#[async_trait]
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;

    /// The error type returned by `Connection`s.
    type Error: failure::Fail;

    /// Attempts to create a new connection.
    ///
    /// Note that boxing is used here since impl Trait is not yet supported
    /// within trait definitions.
    async fn connect(&self) -> Result<Self::Connection, L337Error<Self::Error>>;

    /// Determines if the connection is still connected to the database.
    ///
    /// Note that boxing is used here since impl Trait is not yet supported
    /// within trait definitions.
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), L337Error<Self::Error>>;

    /// Quick check to determine if the connection has broken
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;

    /// Produce an error representing a connection timeout.
    fn timed_out(&self) -> L337Error<Self::Error>;
}
