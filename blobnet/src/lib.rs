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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use hyper::server::conn::AddrIncoming;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use thiserror::Error;
use tokio::{fs, time};

use crate::handler::handle;
use crate::provider::Provider;

pub mod client;
mod handler;
pub mod provider;
mod utils;

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
    IO(#[from] std::io::Error),

    /// An operational error occurred in blobnet.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Default)]
pub struct BlobnetBuilder {
    providers: Vec<Box<dyn Provider>>,
    cache: Option<(PathBuf, u64)>,
}

impl BlobnetBuilder {
    pub fn provider(&mut self, provider: impl Provider + 'static) -> &mut Self {
        self.providers.push(Box::new(provider));
        self
    }

    pub fn cache(&mut self, path: impl AsRef<Path>, size: u64) -> &mut Self {
        self.cache = Some((path.as_ref().into(), size));
        self
    }

    pub async fn build(self) -> Result<Blobnet> {
        let cache = match self.cache {
            Some((path, size)) => Some(Cache::new(path, size).await?),
            None => None,
        };
        Ok(Blobnet {
            providers: self.providers,
            cache,
        })
    }
}

pub struct Blobnet {
    providers: Vec<Box<dyn Provider>>,
    cache: Cache,
}

impl Blobnet {
    pub fn builder() -> BlobnetBuilder {
        Default::default()
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

/// A background process that periodically cleans the cache directory.
///
/// Since the cache directory is limited in size but local to the machine, it is
/// acceptable to delete files from this folder at any time. Therefore, we can
/// simply remove 1/(256^2) of all files at an interval of 60 seconds.
///
/// Doing the math, it would take (256^2) / 60 / 24 = ~46 days on average to
/// expire any given file from the disk cache directory.
pub async fn cleaner(config: Config) {
    const CLEAN_INTERVAL: Duration = Duration::from_secs(30);
    loop {
        time::sleep(CLEAN_INTERVAL).await;
        let prefix = fastrand::u16(..);
        let (d1, d2) = (prefix / 256, prefix % 256);
        let subfolder = config.storage_path.join(&format!("{d1:x}/{d2:x}"));
        if fs::metadata(&subfolder).await.is_ok() {
            println!("cleaning cache directory: {}", subfolder.display());
            let subfolder_tmp = config.storage_path.join(&format!("{d1:x}/.tmp-{d2:x}"));
            fs::remove_dir_all(&subfolder_tmp).await.ok();
            if fs::rename(&subfolder, &subfolder_tmp).await.is_ok() {
                fs::remove_dir_all(&subfolder_tmp).await.ok();
            }
        }
    }
}
