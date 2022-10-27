//! Hyper service handler for the file server.

use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use hyper::body::{Bytes, HttpBody};
use hyper::header::HeaderValue;
use hyper::{Body, Method, Request, Response, StatusCode};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task;

use crate::utils::{chunked_body, get_hash, hash_path};
use crate::Config;

/// The maximum size of a single file that can be stored (16 GB).
const MAX_FILE_SIZE: u64 = u64::pow(2, 34);

/// Parse an HTTP Range header of the format "X-Bn-Range: <start>-<end>".
///
/// The start index is inclusive, and the end index is exclusive. This differs
/// from the standard HTTP `Range` header, which has both ends inclusive.
fn parse_range_header(s: &HeaderValue) -> Option<(u64, u64)> {
    let s = s.to_str().ok()?;
    let (start, end) = s.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

const E500: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;

/// Reads the range-inclusive data from a file with the given hash.
fn read_data(
    config: &Config,
    hash: &str,
    range: Option<(u64, u64)>,
) -> Result<Response<Body>, StatusCode> {
    /// Loads a file, populating it from the NFS if not locally present.
    fn load_file(config: &Config, hash: &str) -> Result<File, StatusCode> {
        let suffix = hash_path(hash).expect("load_file called with invalid hash");
        let path = config.storage_path.join(&suffix);
        if !path.exists() {
            let nfs_path = config.nfs_path.join(&suffix);
            if nfs_path.exists() {
                // atomic_copy(&nfs_path, &path).map_err(|_| E500)?;
            } else {
                return Err(StatusCode::NOT_FOUND);
            }
        }
        File::open(&path).map_err(|_| E500)
    }

    let mut file = load_file(config, hash)?;
    let len = file.metadata().map_err(|_| E500)?.len();

    let limit = if let Some((start, end)) = range {
        // Validate the range is within the file.
        if start > end || end > len {
            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
        }
        file.seek(SeekFrom::Start(start))
            .map_err(|_| StatusCode::RANGE_NOT_SATISFIABLE)?;
        end - start
    } else {
        u64::MAX
    };

    let reader = tokio::fs::File::from_std(file).take(limit);
    let response = Response::builder()
        .header("X-Bn-File-Length", len.to_string())
        .status(StatusCode::OK)
        .body(chunked_body(reader))
        .map_err(|_| E500)?;

    Ok(response)
}

/// Main service handler for the file server.
pub async fn handle(config: Arc<Config>, req: Request<Body>) -> Result<Response<Body>, StatusCode> {
    let secret = req.headers().get("x-bn-secret");
    let secret = secret.and_then(|s| s.to_str().ok());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(Body::from("blobnet ok"))),
        _ if secret != Some(&config.secret) => Err(StatusCode::UNAUTHORIZED),
        (&Method::GET, path) => {
            let range = req.headers().get("x-bn-range").and_then(parse_range_header);
            let hash = String::from(get_hash(path)?);
            task::spawn_blocking(move || read_data(&config, &hash, range))
                .await
                .map_err(|_| E500)?
        }
        (&Method::PUT, "/") => {
            // Spawn a blocking task to write the file.
            let (tx, mut rx) = mpsc::channel::<Option<Bytes>>(32);

            let persist_task = task::spawn_blocking(move || {
                let file = NamedTempFile::new()?;
                let mut writer = BufWriter::new(file);
                let mut hash = Sha256::new();
                let mut done = false;
                while let Some(option) = rx.blocking_recv() {
                    if let Some(chunk) = option {
                        writer.write_all(&chunk)?;
                        hash.update(&chunk);
                    } else {
                        // Was sent a `None` value, meaning that the upload is done.
                        done = true;
                        break;
                    }
                }
                if !done {
                    bail!("upload was interrupted");
                }
                let hash = format!("{:x}", hash.finalize());
                let suffix = hash_path(&hash).expect("persister had invalid hash");
                let file = writer.into_inner()?;
                let nfs_path = config.nfs_path.join(&suffix);
                let storage_path = config.storage_path.join(&suffix);
                fs::create_dir_all(nfs_path.parent().context("nfs path has no parent")?)?;
                // atomic_copy(file.path(), &nfs_path)?;

                // This copy populates the local cache with the newly uploaded file. It is OK if
                // this operation fails, since the ultimate source of truth is in NFS.
                file.persist_noclobber(&storage_path).ok();
                anyhow::Ok(hash)
            });

            let persist_task = async {
                let result = persist_task.await;
                match &result {
                    // These error cases should not happen (internal errors); they are intended to
                    // help with debugging during development.
                    Err(err) => eprintln!("internal error waiting for persist task: {err:?}"),
                    Ok(Err(err)) => eprintln!("internal error in persist task: {err:?}"),
                    _ => (),
                };
                result
            };

            let mut body = req.into_body();
            let mut size = 0;
            while let Some(result) = body.data().await {
                let chunk = result.map_err(|_| StatusCode::BAD_REQUEST)?;
                size += chunk.len() as u64;
                if size > MAX_FILE_SIZE {
                    return Err(StatusCode::PAYLOAD_TOO_LARGE);
                }
                tx.send(Some(chunk)).await.map_err(|_| E500)?;
            }
            tx.send(None).await.map_err(|_| E500)?;

            let hash = persist_task.await.map_err(|_| E500)?.map_err(|_| E500)?;
            Ok(Response::new(Body::from(hash)))
        }
        _ => Err(StatusCode::NOT_FOUND),
    }
}
