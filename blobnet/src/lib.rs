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

use hyper::{Body, Response, StatusCode};
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
    ///
    /// This occurs if the start is greater than the end of the range, or if the
    /// start is past the file's length. It's okay for the end of the range to
    /// be past the end of the file; the output will be truncated.
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

impl From<Error> for Response<Body> {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound => make_resp(StatusCode::NOT_FOUND, err.to_string()),
            Error::BadRange => make_resp(StatusCode::RANGE_NOT_SATISFIABLE, err.to_string()),
            Error::IO(_) => make_resp(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            Error::Internal(_) => make_resp(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
    }
}

/// Helper function for making a simple text response.
fn make_resp(code: StatusCode, msg: impl Into<String>) -> Response<Body> {
    Response::builder()
        .status(code)
        .body(Body::from(msg.into() + "\n"))
        .unwrap()
}
