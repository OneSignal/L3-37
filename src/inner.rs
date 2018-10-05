use crossbeam::queue::SegQueue;
use futures::sync::oneshot;

use manage_connection::ManageConnection;
use queue::{Live, Queue};

// Most of this comes from c3po's inner module: https://github.com/withoutboats/c3po/blob/08a6fde00c6506bacfe6eebe621520ee54b418bb/src/inner.rs
// with some additions and updates to work with modern versions of tokio

#[derive(Debug)]
pub struct ConnectionPool<C: ManageConnection> {
    conns: Queue<C::Connection>,
    waiting: SegQueue<oneshot::Sender<Live<C::Connection>>>,
    manager: C,
}

impl<C: ManageConnection> ConnectionPool<C> {
    pub fn new(conns: Queue<C::Connection>, manager: C) -> ConnectionPool<C> {
        ConnectionPool {
            conns: conns,
            waiting: SegQueue::new(),
            manager,
        }
    }

    pub fn get_connection(&self) -> Option<Live<C::Connection>> {
        self.conns.get()
    }

    pub fn notify_of_connection(&self, tx: oneshot::Sender<Live<C::Connection>>) {
        self.waiting.push(tx);
    }

    /// The total number of connections in the pool.
    pub fn total_conns(&self) -> usize {
        self.conns.total()
    }

    /// The number of idle connections in the pool.
    pub fn idle_conns(&self) -> usize {
        self.conns.idle()
    }

    // /// Prepare the reap job to run on the event loop.
    // pub fn prepare_reaper(this: &Arc<Self>) {
    //     let pool = this.clone();
    //     this.remote.spawn(|handle| {
    //         Interval::new(pool.config.reap_frequency, handle)
    //             .into_future()
    //             .and_then(|interval| {
    //                 interval.for_each(move |_| {
    //                     InnerPool::reap_and_replenish(&pool);
    //                     Ok(())
    //                 })
    //             }).map_err(|_| ())
    //     });
    // }

    // /// Create a new connection and store it in the pool.
    // pub fn replenish_connection(&self, pool: Arc<Self>) {
    //     let spawn = self.new_connection().map_err(|_| ()).map(move |conn| {
    //         pool.increment();
    //         InnerPool::store(&pool, Live::new(conn))
    //     });
    //     self.remote.spawn(|_| spawn);
    // }

    // /// Get a connection from the pool.
    // pub fn get_connection(&self) -> Option<Live<C::Instance>> {
    //     self.conns.get()
    // }

    // /// Create and return a new connection.
    // pub fn new_connection(&self) -> C::Future {
    //     self.client.new_service()
    // }

    // /// Prepare to notify this sender of an available connection.
    // pub fn notify_of_connection(&self, tx: oneshot::Sender<Live<C::Instance>>) {
    //     self.waiting.push(tx);
    // }

    // /// The timeout for waiting on a new connection.
    // pub fn connection_timeout(&self) -> Option<oneshot::Receiver<()>> {
    //     self.config.connect_timeout.map(|duration| {
    //         let (tx, rx) = oneshot::channel();
    //         self.remote.spawn(move |handle| {
    //             Timeout::new(duration, handle)
    //                 .into_future()
    //                 .map_err(|_| ())
    //                 .and_then(|timeout| timeout.map_err(|_| ()).and_then(move |_| tx.send(())))
    //         });
    //         rx
    //     })
    // }

    /// Receive a connection back to be stored in the pool. This could have one
    /// of three outcomes:
    /// * The connection will be released, if it should be released.
    /// * The connection will be passed to a waiting future, if any exist.
    /// * The connection will be put back into the connection pool.
    pub fn store(&self, conn: Live<C::Connection>) {
        // Otherwise, first attempt to send it to any waiting requests
        let mut conn = conn;
        while let Some(waiting) = self.waiting.try_pop() {
            conn = match waiting.send(conn) {
                Ok(_) => return,
                Err(conn) => conn,
            };
        }
        // If there are no waiting requests & we aren't over the max idle
        // connections limit, attempt to store it back in the pool
        self.conns.store(conn);
    }

    // /// Increment the connection count.
    // pub fn increment(&self) {
    //     self.conns.increment();
    // }

    // pub fn reap_and_replenish(this: &Arc<Self>) {
    //     debug_assert!(
    //         this.total() >= this.idle(),
    //         "total ({}) < idle ({})",
    //         this.total(),
    //         this.idle()
    //     );
    //     debug_assert!(
    //         this.max_conns().map_or(true, |max| this.total() <= max),
    //         "total ({}) > max_conns ({})",
    //         this.total(),
    //         this.max_conns().unwrap()
    //     );
    //     debug_assert!(
    //         this.total() >= this.min_conns(),
    //         "total ({}) < min_conns ({})",
    //         this.total(),
    //         this.min_conns()
    //     );
    //     this.reap();
    //     InnerPool::replenish(this);
    // }

    // /// Reap connections.
    // fn reap(&self) {
    //     self.conns.reap(&self.config);
    // }

    // /// Replenish connections after finishing reaping.
    // fn replenish(this: &Arc<Self>) {
    //     // Create connections (up to max) for each request waiting for notifications
    //     if let Some(max) = this.max_conns() {
    //         let mut ctr = max - this.total();
    //         while let Some(waiting) = this.waiting.try_pop() {
    //             let pool = this.clone();
    //             let spawn = this.new_connection().map_err(|_| ()).map(move |conn| {
    //                 let conn = Live::new(conn);
    //                 if let Err(conn) = waiting.send(conn) {
    //                     InnerPool::store(&pool, conn)
    //                 }
    //             });
    //             this.remote.spawn(|_| spawn);
    //             ctr -= 1;
    //             if ctr == 0 {
    //                 break;
    //             }
    //         }
    //     } else {
    //         while let Some(waiting) = this.waiting.try_pop() {
    //             let pool = this.clone();
    //             let spawn = this.new_connection().map_err(|_| ()).map(move |conn| {
    //                 let conn = Live::new(conn);
    //                 if let Err(conn) = waiting.send(conn) {
    //                     InnerPool::store(&pool, conn)
    //                 }
    //             });
    //             this.remote.spawn(|_| spawn);
    //         }
    //     }

    //     // Create connections until we have the minimum number of connections
    //     for _ in this.total()..this.config.min_connections {
    //         this.replenish_connection(this.clone());
    //     }

    //     // Create connections until we have the minimum number of idle connections
    //     if let Some(min_idle_connections) = this.config.min_idle_connections {
    //         for _ in this.conns.idle()..min_idle_connections {
    //             this.replenish_connection(this.clone());
    //         }
    //     }
    // }

    // /// The maximum connections allowed in the pool.
    // pub fn max_conns(&self) -> Option<usize> {
    //     self.config.max_connections
    // }

    // /// The minimum connections allowed in the pool.
    // fn min_conns(&self) -> usize {
    //     self.config.min_connections
    // }
}
