use futures;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Future canceled while waiting to connect: {}", _0)]
    PoolCanceled(futures::Canceled),
}
