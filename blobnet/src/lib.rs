//! A low-latency file server that responds to requests for chunks of file data.
//!
//! This acts as a non-volatile, over-the-network content cache. Internal users
//! can add binary blobs to the cache, and the data is indexed by its SHA-256
//! hash. Any blob can be retrieved by its hash and range of bytes to read.
//!
//! Data stored in blobnet is locally cached and durable.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::io;
use std::pin::Pin;

use hyper::StatusCode;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

pub mod client;
pub mod provider;
pub mod server;
mod utils;

#[allow(clippy::declare_interior_mutable_const)]
mod headers {
    use hyper::header::HeaderName;

    pub const HEADER_SECRET: HeaderName = HeaderName::from_static("x-bn-secret");
    pub const HEADER_RANGE: HeaderName = HeaderName::from_static("x-bn-range");
    pub const HEADER_FILE_LENGTH: HeaderName = HeaderName::from_static("x-bn-file-length");
}

/// A stream of bytes from some data source.
pub type ReadStream = Pin<Box<dyn AsyncRead + Send>>;

/// Helper function to collect a [`ReadStream`] into a byte vector.
pub async fn read_to_vec(mut stream: ReadStream) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}

/// Error type for results returned from blobnet.
#[derive(Error, Debug)]
pub enum Error {
    /// The requested file was not found.
    #[error("file not found")]
    NotFound,

    /// The requested range was not satisfiable.
    #[error("range not satisfiable")]
    BadRange,

    /// An error in network or filesystem communication occurred.
    #[error(transparent)]
    IO(#[from] io::Error),

    /// An operational error occurred in blobnet.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::IO(err) => err,
            _ => io::Error::new(io::ErrorKind::Other, err),
        }
    }
}

impl From<Error> for StatusCode {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::BadRange => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::IO(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
