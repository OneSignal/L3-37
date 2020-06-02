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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crossbeam_queue::{ArrayQueue, PushError};

// Almost all of this file is directly from c3po: https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/queue.rs

/// A connection, carrying with it a record of how long it has been live.
#[derive(Debug)]
pub struct Live<T: Send> {
    pub conn: T,
    pub live_since: Instant,
}

impl<T: Send> Live<T> {
    pub fn new(conn: T) -> Live<T> {
        Live {
            conn,
            live_since: Instant::now(),
        }
    }
}

/// An idle connection, carrying with it a record of how long it has been idle.
#[derive(Debug)]
pub(crate) struct Idle<T: Send> {
    conn: Live<T>,
    idle_since: Instant,
}

impl<T: Send> Idle<T> {
    fn new(conn: Live<T>) -> Idle<T> {
        Idle {
            conn,
            idle_since: Instant::now(),
        }
    }
}

/// A queue of idle connections which counts how many connections exist total
/// (including those which are not in the queue.)
#[derive(Debug)]
pub struct Queue<C: Send> {
    idle: ArrayQueue<Idle<C>>,
    total_count: AtomicUsize,
    idle_push_error_count: AtomicUsize,
}

impl<C: Send> Queue<C> {
    /// Construct an empty queue with a certain capacity
    pub(crate) fn new(capacity: usize) -> Queue<C> {
        Queue {
            idle: ArrayQueue::new(capacity),
            total_count: AtomicUsize::new(0),
            idle_push_error_count: AtomicUsize::new(0),
        }
    }

    /// Count of idle connection in queue
    #[inline(always)]
    pub(crate) fn idle(&self) -> usize {
        self.idle.len()
    }

    /// Count of total connections active
    #[inline(always)]
    pub(crate) fn total(&self) -> usize {
        self.total_count.load(Ordering::SeqCst)
    }

    /// Error counter incremented in `store()` method above.
    pub(crate) fn idle_push_error_count(&self) -> usize {
        self.idle_push_error_count.load(Ordering::Relaxed)
    }

    /// Push a new connection into the queue (this will increment
    /// the total connection count).
    pub(crate) fn new_conn(&self, conn: Live<C>) {
        if self.store(conn).is_ok() {
            self.increment();
        }
    }

    /// Store a connection which has already been counted in the queue
    /// (this will NOT increment the total connection count).
    pub(crate) fn store(&self, conn: Live<C>) -> Result<(), PushError<Idle<C>>> {
        if let Err(err) = self.idle.push(Idle::new(conn)) {
            self.idle_push_error_count.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        Ok(())
    }

    /// Get the longest-idle connection from the queue.
    pub(crate) fn get(&self) -> Option<Live<C>> {
        self.idle.pop().ok().map(|Idle { conn, .. }| conn)
    }

    /// Increment the connection count without pushing a connection into the
    /// queue.
    #[inline(always)]
    pub(crate) fn increment(&self) {
        self.total_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the connection count
    #[inline(always)]
    pub(crate) fn decrement(&self) {
        self.total_count.fetch_sub(1, Ordering::SeqCst);
        // this is commented out because it was cuasing an overflow. it's probably important that
        // this is actually run
        // self.idle_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Increment the total number of connections safely, with guarantees that we won't increment
    /// past `max`. This does block until max is reached, so don't pass a huge max size and expect
    /// it to return quickly.
    pub(crate) fn safe_increment(&self, max: usize) -> Option<()> {
        let mut curr_count = self.total();
        while curr_count < max {
            match self.total_count.compare_exchange(
                curr_count,
                curr_count + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // If we won the race, return that we did and stop trying
                    return Some(());
                }
                Err(_) => {
                    // If didn't win the race, get the current count again and try to do the whole
                    // thing again
                    curr_count = self.total();
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_conn() {
        let conns = Queue::new(1);
        assert_eq!(conns.idle(), 0);
        assert_eq!(conns.total(), 0);
        conns.new_conn(Live::new(()));
        assert_eq!(conns.idle(), 1);
        assert_eq!(conns.total(), 1);
    }

    #[test]
    fn store() -> Result<(), crossbeam_queue::PushError<Idle<()>>> {
        let conns = Queue::new(1);
        assert_eq!(conns.idle(), 0);
        assert_eq!(conns.total(), 0);
        conns.store(Live::new(()))?;
        assert_eq!(conns.idle(), 1);
        assert_eq!(conns.total(), 0);
        Ok(())
    }

    #[test]
    fn get() {
        let conns = Queue::new(1);
        assert!(conns.get().is_none());
        conns.new_conn(Live::new(()));
        assert!(conns.get().is_some());
        assert_eq!(conns.idle(), 0);
        assert_eq!(conns.total(), 1);
    }

    #[test]
    fn increment_and_decrement() {
        let conns: Queue<()> = Queue::new(1);
        assert_eq!(conns.total(), 0);
        assert_eq!(conns.idle(), 0);
        conns.increment();
        assert_eq!(conns.total(), 1);
        assert_eq!(conns.idle(), 0);
        conns.decrement();
        assert_eq!(conns.total(), 0);
        assert_eq!(conns.idle(), 0);
    }

    #[test]
    fn store_queue_more_than_the_capacity() {
        let conns: Queue<()> = Queue::new(1);
        conns.new_conn(Live::new(()));
        assert_eq!(1, conns.total());
        assert_eq!(1, conns.idle());
        assert_eq!(0, conns.idle_push_error_count());
        conns.new_conn(Live::new(()));
        assert_eq!(1, conns.total());
        assert_eq!(1, conns.idle());
        assert_eq!(1, conns.idle_push_error_count());
    }
}
