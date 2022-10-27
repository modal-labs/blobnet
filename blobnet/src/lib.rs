//! A low-latency file server that responds to requests for chunks of file data.
//!
//! This acts as a non-volatile, over-the-network content cache. Internal users
//! can add binary blobs to the cache, and the data is indexed by its SHA-256
//! hash. Any blob can be retrieved by its hash and range of bytes to read.
//!
//! Data stored in blobnet is locally cached and durable.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::convert::Infallible;
use std::future::{self, Future};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
pub use hyper::server::conn::AddrIncoming;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::{fs, time};

use crate::handler::handle;
use crate::provider::Provider;

pub mod client;
mod handler;
pub mod provider;
mod utils;

/// A stream of bytes from some data source.
pub type ReadStream = Pin<Box<dyn AsyncRead + Send>>;

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

/// Configuration for the file server.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the local disk storage.
    pub storage_path: PathBuf,

    /// Path to the network file system mount.
    pub nfs_path: PathBuf,

    /// Secret used to authorize users to access the service.
    pub secret: String,
}

/// Create a file server listening at the given address.
pub async fn listen(config: Config, incoming: AddrIncoming) -> Result<(), hyper::Error> {
    listen_with_shutdown(config, incoming, future::pending()).await
}

/// Create a file server listening at the given address, with graceful shutdown.
pub async fn listen_with_shutdown(
    config: Config,
    incoming: AddrIncoming,
    shutdown: impl Future<Output = ()>,
) -> Result<(), hyper::Error> {
    let config = Arc::new(config);

    // Low-level service boilerplate to interface with the [`hyper`] API.
    let make_svc = make_service_fn(move |_conn| {
        let config = Arc::clone(&config);
        async {
            Ok::<_, Infallible>(service_fn(move |req| {
                let config = Arc::clone(&config);
                async {
                    let resp = handle(config, req).await;
                    Ok::<_, Infallible>(resp.unwrap_or_else(|code| {
                        Response::builder()
                            .status(code)
                            .body(Body::empty())
                            .unwrap()
                    }))
                }
            }))
        }
    });

    Server::builder(incoming)
        .tcp_nodelay(true)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown)
        .await
}
