//! File system and hash utilities used by the file server.

use std::fs;
use std::io::{self, BufWriter, ErrorKind, Read};
use std::path::Path;

use anyhow::{anyhow, ensure, Result};
use hyper::Body;
use tempfile::NamedTempFile;
use tokio_stream::StreamExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{Error, ReadStream};

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Extracts the hash from the path component of an HTTP request URL.
pub(crate) fn get_hash(path: &str) -> Result<&str, Error> {
    match path.strip_prefix('/') {
        Some(hash) if is_hash(hash) => Ok(hash),
        _ => Err(Error::NotFound),
    }
}

/// Checks if a hash is valid and returns and error if not.
pub(crate) fn check_hash(hash: &str) -> Result<()> {
    ensure!(is_hash(hash), "received an invalid SHA-256 hash: {hash}");
    Ok(())
}

/// Obtain a file content path from a hexadecimal SHA-256 hash.
pub(crate) fn hash_path(hash: &str) -> Result<String> {
    check_hash(hash)?;

    Ok(format!(
        "{}/{}/{}/{}",
        &hash[0..2],
        &hash[2..4],
        &hash[4..6],
        &hash[6..],
    ))
}

/// Copies a file to a destination location, atomically, without clobbering any
/// data if a file already exists at that location.
///
/// Creates a temporary file at the destination address to avoid issues with
/// moves failing between different mounted file systems.
///
/// The destination must have a parent directory (path cannot terminate in a
/// root or prefix), and this parent directory along with its ancestors are also
/// created if they do not already exist.
///
/// Returns `true` if the file was not previously present at the destination and
/// was written successfully.
pub(crate) fn atomic_copy(mut source: impl Read, dest: impl AsRef<Path>) -> Result<bool> {
    let dest = dest.as_ref();

    if fs::metadata(dest).is_err() {
        let parent = dest
            .parent()
            .ok_or_else(|| anyhow!("parent of destination path {dest:?} does not exist"))?
            .to_owned();

        fs::create_dir_all(&parent)?;
        let mut file = NamedTempFile::new_in(parent)?;
        {
            let mut writer = BufWriter::with_capacity(1 << 21, &mut file);
            std::io::copy(&mut source, &mut writer)?;
        }

        if let Err(err) = file.persist_noclobber(dest) {
            // Ignore error if another caller created the file in the meantime.
            if err.error.kind() != ErrorKind::AlreadyExists {
                return Err(err.into());
            }
            Ok(false)
        } else {
            Ok(true)
        }
    } else {
        Ok(false)
    }
}

/// Convert a [`ReadStream`] object into an HTTP body.
pub(crate) fn stream_body(stream: ReadStream) -> Body {
    Body::wrap_stream(ReaderStream::new(stream))
}

/// Convert an HTTP body into a [`ReadStream`] object.
pub(crate) fn body_stream(body: Body) -> ReadStream {
    Box::pin(StreamReader::new(StreamExt::map(body, |result| {
        result.map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    })))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::hash_path;

    #[test]
    fn test_hash_path() -> Result<()> {
        assert_eq!(
            hash_path("f2252f951decf449ea1b5e773a7750650ac01cd159a5fc8e04e56dbf2c06e091")?,
            "f2/25/2f/951decf449ea1b5e773a7750650ac01cd159a5fc8e04e56dbf2c06e091",
        );
        assert!(hash_path(&"a".repeat(64)).is_ok());
        assert!(hash_path(&"A".repeat(64)).is_err());
        assert!(hash_path("gk3ipjgpjg2pjog").is_err());
        Ok(())
    }
}
