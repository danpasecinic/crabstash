use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Corruption: {0}")]
    Corruption(String),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Transaction conflict")]
    TransactionConflict,

    #[error("Transaction aborted")]
    TransactionAborted,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, Error>;
