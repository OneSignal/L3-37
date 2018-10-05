// #![deny(missing_docs)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crossbeam::queue::SegQueue;

// Almost all of this file is directly from c3po: https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/queue.rs

/// A connection, carrying with it a record of how long it has been live.
#[derive(Debug)]
pub struct Live<T> {
    pub conn: T,
    pub live_since: Instant,
}

impl<T> Live<T> {
    pub fn new(conn: T) -> Live<T> {
        Live {
            conn: conn,
            live_since: Instant::now(),
        }
    }
}

/// An idle connection, carrying with it a record of how long it has been idle.
#[derive(Debug)]
struct Idle<T> {
    conn: Live<T>,
    idle_since: Instant,
}

impl<T> Idle<T> {
    fn new(conn: Live<T>) -> Idle<T> {
        Idle {
            conn: conn,
            idle_since: Instant::now(),
        }
    }
}

/// A queue of idle connections which counts how many connections exist total
/// (including those which are not in the queue.)
#[derive(Debug)]
pub struct Queue<C> {
    idle: SegQueue<Idle<C>>,
    idle_count: AtomicUsize,
    total_count: AtomicUsize,
}

impl<C> Queue<C> {
    /// Construct an empty queue with a certain capacity
    pub fn new() -> Queue<C> {
        Queue {
            idle: SegQueue::new(),
            idle_count: AtomicUsize::new(0),
            total_count: AtomicUsize::new(0),
        }
    }

    /// Count of idle connection in queue
    #[inline(always)]
    pub fn idle(&self) -> usize {
        self.idle_count.load(Ordering::SeqCst)
    }

    /// Count of total connections active
    #[inline(always)]
    pub fn total(&self) -> usize {
        self.total_count.load(Ordering::SeqCst)
    }

    /// Push a new connection into the queue (this will increment
    /// the total connection count).
    pub fn new_conn(&self, conn: Live<C>) {
        self.store(conn);
        self.increment();
    }

    /// Store a connection which has already been counted in the queue
    /// (this will NOT increment the total connection count).
    pub fn store(&self, conn: Live<C>) {
        self.idle_count.fetch_add(1, Ordering::SeqCst);
        self.idle.push(Idle::new(conn));
    }

    /// Get the longest-idle connection from the queue.
    pub fn get(&self) -> Option<Live<C>> {
        self.idle.try_pop().map(|Idle { conn, .. }| {
            self.idle_count.fetch_sub(1, Ordering::SeqCst);
            conn
        })
    }

    /// Increment the connection count without pushing a connection into the
    /// queue.
    #[inline(always)]
    pub fn increment(&self) {
        self.total_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the connection count
    #[inline(always)]
    pub fn decrement(&self) {
        self.total_count.fetch_sub(1, Ordering::SeqCst);
        // this is commented out because it was cuasing an overflow. it's probably important that
        // this is actually run
        // self.idle_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Increment the total number of connections safely, with guarantees that we won't increment
    /// past `max`. This does block until max is reached, so don't pass a huge max size and expect
    /// it to return quickly.
    pub fn safe_increment(&self, max: usize) -> Option<()> {
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
        let conns = Queue::new();
        assert_eq!(conns.idle(), 0);
        assert_eq!(conns.total(), 0);
        conns.new_conn(Live::new(()));
        assert_eq!(conns.idle(), 1);
        assert_eq!(conns.total(), 1);
    }

    #[test]
    fn store() {
        let conns = Queue::new();
        assert_eq!(conns.idle(), 0);
        assert_eq!(conns.total(), 0);
        conns.store(Live::new(()));
        assert_eq!(conns.idle(), 1);
        assert_eq!(conns.total(), 0);
    }

    #[test]
    fn get() {
        let conns = Queue::new();
        assert!(conns.get().is_none());
        conns.new_conn(Live::new(()));
        assert!(conns.get().is_some());
        assert_eq!(conns.idle(), 0);
        assert_eq!(conns.total(), 1);
    }

    #[test]
    fn increment_and_decrement() {
        let conns: Queue<()> = Queue::new();
        assert_eq!(conns.total(), 0);
        assert_eq!(conns.idle(), 0);
        conns.increment();
        assert_eq!(conns.total(), 1);
        assert_eq!(conns.idle(), 0);
        conns.decrement();
        assert_eq!(conns.total(), 0);
        assert_eq!(conns.idle(), 0);
    }
}
