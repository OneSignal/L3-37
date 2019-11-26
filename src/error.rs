use failure::Fail;

#[derive(Debug, Fail)]
pub enum InternalError {
    #[fail(display = "unknown error: {}", _0)]
    Other(String),
}
