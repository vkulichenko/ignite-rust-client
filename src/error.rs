use crate::Version;

pub(crate) type Result<T> = core::result::Result<T, Error>;

#[derive(PartialEq, Debug)]
pub enum ErrorKind {
    Network,
    Serde,
    Handshake { server_version: Version, client_version: Version },
    Ignite(i32),
}

#[derive(PartialEq, Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: String) -> Error {
        Error { kind, message }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Error {
        Error { kind: ErrorKind::Network, message: error.to_string() }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(error: std::string::FromUtf8Error) -> Error {
        Error { kind: ErrorKind::Serde, message: error.to_string() }
    }
}
