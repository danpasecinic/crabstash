use std::fmt;

pub use eyre::Result;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Corruption(String),
    KeyNotFound,
    TransactionConflict,
    TransactionAborted,
    InvalidArgument(String),
    LockTimeout,
    Deadlock,
    LockConflict,
    WriteSkew,
    PhantomRead,
    SerializableConflict,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(msg) => write!(f, "Corruption: {msg}"),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::TransactionConflict => write!(f, "Transaction conflict"),
            Error::TransactionAborted => write!(f, "Transaction aborted"),
            Error::InvalidArgument(msg) => write!(f, "Invalid argument: {msg}"),
            Error::LockTimeout => write!(f, "Lock acquisition timed out"),
            Error::Deadlock => write!(f, "Deadlock detected"),
            Error::LockConflict => write!(f, "Lock conflict"),
            Error::WriteSkew => write!(f, "Write skew detected (serializable violation)"),
            Error::PhantomRead => write!(f, "Phantom read detected (serializable violation)"),
            Error::SerializableConflict => {
                write!(f, "Serializable conflict (dangerous structure detected)")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}
