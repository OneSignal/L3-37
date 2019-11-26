#[derive(Debug)]
pub enum InternalError {
    Other(String),
}

impl std::error::Error for InternalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            InternalError::Other(string) => write!(f, "{}", string),
        }
    }
}
