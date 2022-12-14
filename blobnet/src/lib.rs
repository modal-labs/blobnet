//! # Blobnet
//!
//! A configurable, low-latency blob storage server for content-addressed data.
//!
//! This acts as a non-volatile, over-the-network content cache. Clients can add
//! binary blobs (fixed-size byte vectors) to the cache, and the data is indexed
//! by its SHA-256 hash. Any blob can be retrieved given its hash and the range
//! of bytes to read.
//!
//! Data stored in blobnet is locally cached and durable.
//!
//! ## Providers
//!
//! The core of blobnet is the [`Provider`] trait. This trait defines the
//! interface shared by all blobnet instances. It is used like so:
//!
//! ```
//! use std::io::Cursor;
//! use blobnet::ReadStream;
//! use blobnet::provider::{self, Provider};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), blobnet::Error> {
//! // Create a new provider.
//! let provider = provider::Memory::new();
//!
//! // Insert data, returning its hash.
//! let data: ReadStream = Box::pin(b"hello blobnet world!" as &[u8]);
//! let hash = provider.put(data).await?;
//!
//! // Check if a blob exists and return its size.
//! let size = provider.head(&hash).await?;
//! assert_eq!(size, 20);
//!
//! // Read the content as a binary stream.
//! provider.get(&hash, None).await?;
//! provider.get(&hash, Some((0, 10))).await?; // Requests the first 10 bytes.
//! # Ok(())
//! # }
//! ```
//!
//! You can combine these operations in any order, and they can run in parallel,
//! since they take shared `&self` receivers. The semantics of each operation
//! should behave the same regardless of provider.
//!
//! The [`Provider`] trait is public, and several providers are offered,
//! supporting storage in a local directory, network file system, or in AWS S3.
//!
//! ## Network Server
//!
//! Blobnet allows you to run it as a server and send data over the network.
//! This serves responses to blob operations over the HTTP/2 protocol. For
//! example, you can run a blobnet server on a local machine with
//!
//! ```bash
//! export BLOBNET_SECRET=my-secret
//! blobnet --source localdir:/tmp/blobnet --port 7609
//! ```
//!
//! This specifies the provider using a string syntax for the `--source` flag.
//! You can connect to the server as a provider in another process:
//!
//! ```
//! use blobnet::{client::FileClient, provider};
//!
//! let client = FileClient::new_http("http://localhost:7609", "my-secret");
//! let provider = provider::Remote::new(client);
//! ```
//!
//! Why would you want to share a blobnet server over the network? One use case
//! is for shared caches.
//!
//! ## Caching
//!
//! Blobnet supports two-tiered caching of data with the [`Cached`] provider.
//! This breaks up files into chunks with a configurable page size, storing them
//! in a local cache directory and an in-memory page cache. By adding a cache in
//! non-volatile storage, we can speed up file operations by multiple orders of
//! magnitude compared to a network file system, such as:
//!
//! ```
//! use blobnet::provider;
//!
//! // Create a new provider targeting a local NFS mount.
//! let provider = provider::LocalDir::new("/mnt/nfs");
//!
//! /// Add a caching layer on top of the provider, with 2 MiB page size.
//! let provider = provider::Cached::new(provider, "/tmp/blobnet-cache", 1 << 21);
//! ```
//!
//! Caching is also useful for accessing remote blobnet servers. It composes
//! well and can add more tiers to the dataflow, improving system efficiency and
//! network load.
//!
//! ```
//! use blobnet::{client::FileClient, provider};
//!
//! // Create a new provider fetching content over the network.
//! let client = FileClient::new_http("http://localhost:7609", "my-secret");
//! let provider = provider::Remote::new(client);
//!
//! /// Add a caching layer on top of the provider, with 2 MiB page size.
//! let provider = provider::Cached::new(provider, "/tmp/blobnet-cache", 1 << 21);
//! ```
//!
//! Together these abstractions allow you to create a configurable, very
//! low-latency content-addressed storage system.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::io;
use std::pin::Pin;

use hyper::{Body, Response, StatusCode};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

#[cfg(doc)]
use crate::provider::{Cached, Provider};

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

/// Internal type alias for a byte range.
type BlobRange = Option<(u64, u64)>;

/// A stream of bytes from some data source.
pub type ReadStream<'a> = Pin<Box<dyn AsyncRead + Send + 'a>>;

/// Helper function to collect a [`ReadStream`] into a byte vector.
pub async fn read_to_vec(mut stream: ReadStream<'_>) -> io::Result<Vec<u8>> {
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
    /// This only occurs if the start is greater than the end of the range. If
    /// the start or end of the range is longer than the file, it will simply be
    /// truncated without an error.
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
