//! A ground-truth storage provider for asynchronous file access.

use std::{collections::HashMap, io::SeekFrom};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use aws_sdk_s3::{
    error::{GetObjectErrorKind, HeadObjectErrorKind},
    types::{ByteStream, SdkError},
};
use hyper::{body::Bytes, client::connect::Connect};
use sha2::{Digest, Sha256};
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::task;

use crate::{
    client::FileClient,
    utils::{chunked_body, hash_path},
    BlobnetError,
};

/// Specifies a storage backend for the blobnet service.
///
/// Each method returns an error only when some operational problem occurs, such
/// as in I/O or communication. Retries should be handled internally by the
/// function since each provider has different failure modes.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Check if a file exists and returns its size in bytes.
    async fn head(&self, hash: &str) -> Result<u64, BlobnetError>;

    /// Returns the data from the file at the given path.
    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<Bytes, BlobnetError>;

    /// Adds a binary blob to storage, returning its hash.
    ///
    /// This function is not as latency-sensitive as the others, caring more
    /// about throughput. It may take two passes over the data.
    async fn put(&self, data: Box<dyn AsyncRead + Send + Unpin>) -> Result<String, BlobnetError>;
}

/// A provider that stores blobs in memory, only for debugging.
#[derive(Default)]
pub struct Memory {
    data: parking_lot::RwLock<HashMap<String, Bytes>>,
}

impl Memory {
    /// Create a new, empty in-memory storage.
    pub fn new() -> Self {
        Default::default()
    }
}

#[async_trait]
impl Provider for Memory {
    async fn head(&self, hash: &str) -> Result<u64, BlobnetError> {
        let data = self.data.read();
        let bytes = data.get(hash).ok_or(BlobnetError::NotFound)?;
        Ok(bytes.len() as u64)
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<Bytes, BlobnetError> {
        let data = self.data.read();
        let mut bytes = match data.get(hash) {
            Some(bytes) => bytes.clone(),
            None => return Err(BlobnetError::NotFound),
        };
        if let Some((start, end)) = range {
            if start > end || end > bytes.len() as u64 {
                return Err(BlobnetError::BadRange);
            }
            bytes = bytes.slice(start as usize..end as usize);
        }
        Ok(bytes)
    }

    async fn put(
        &self,
        mut data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<String, BlobnetError> {
        let mut buf = Vec::new();
        data.read_to_end(&mut buf)
            .await
            .map_err(anyhow::Error::from)?;
        let hash = format!("{:x}", Sha256::new().chain_update(&buf).finalize());
        self.data.write().insert(hash.clone(), Bytes::from(buf));
        Ok(hash)
    }
}

/// A provider that stores blobs in an S3 bucket.
pub struct S3 {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3 {
    /// Creates a new S3 provider.
    pub async fn new(client: aws_sdk_s3::Client, bucket: &str) -> anyhow::Result<Self> {
        client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .with_context(|| format!("unable to create provider for S3 bucket {bucket}"))?;
        Ok(Self {
            client,
            bucket: bucket.into(),
        })
    }
}

#[async_trait]
impl Provider for S3 {
    async fn head(&self, hash: &str) -> Result<u64, BlobnetError> {
        let key = hash_path(hash)?;
        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(resp) => Ok(resp.content_length() as u64),
            Err(SdkError::ServiceError { err, .. })
                if matches!(err.kind, HeadObjectErrorKind::NotFound(_)) =>
            {
                Err(BlobnetError::NotFound)
            }
            Err(err) => Err(BlobnetError::Internal(err.into())),
        }
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<Bytes, BlobnetError> {
        check_range(range)?;
        let key = hash_path(hash)?;
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .set_range(range.map(|(start, end)| format!("{}-{}", start, end - 1)))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let result = resp.body.collect().await;
                Ok(result.map_err(anyhow::Error::from)?.into_bytes())
            }
            Err(SdkError::ServiceError { err, .. })
                if matches!(err.kind, GetObjectErrorKind::NoSuchKey(_)) =>
            {
                Err(BlobnetError::NotFound)
            }
            Err(err) => Err(BlobnetError::Internal(err.into())),
        }
    }

    async fn put(&self, data: Box<dyn AsyncRead + Send + Unpin>) -> Result<String, BlobnetError> {
        let (hash, file) = make_data_tempfile(data).await?;
        let body = ByteStream::read_from()
            .file(file)
            .build()
            .await
            .map_err(anyhow::Error::from)?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(hash_path(&hash)?)
            .checksum_sha256(&hash)
            .body(body)
            .send()
            .await
            .map_err(anyhow::Error::from)?;
        Ok(hash)
    }
}

/// A provider that stores blobs in a local, NFS-mounted directory.
pub struct NFS;

#[async_trait]
impl Provider for NFS {
    async fn head(&self, hash: &str) -> Result<u64, BlobnetError> {
        todo!()
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<Bytes, BlobnetError> {
        todo!()
    }

    async fn put(&self, data: Box<dyn AsyncRead + Send + Unpin>) -> Result<String, BlobnetError> {
        todo!()
    }
}

/// A provider that routes requests to a remote blobnet server.
pub struct Remote<C> {
    client: FileClient<C>,
}

impl<C> Remote<C> {
    /// Construct a new remote provider using the given client.
    pub fn new(client: FileClient<C>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<C: Connect + Clone + Send + Sync + 'static> Provider for Remote<C> {
    async fn head(&self, hash: &str) -> Result<u64, BlobnetError> {
        self.client.head(hash).await
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<Bytes, BlobnetError> {
        self.client.get(hash, range).await
    }

    async fn put(&self, data: Box<dyn AsyncRead + Send + Unpin>) -> Result<String, BlobnetError> {
        let (hash, file) = make_data_tempfile(data).await?;
        self.client
            .put(|| async { Ok(chunked_body(file.try_clone().await?)) })
            .await
    }
}

/// Stream data from a source into a temporary file and compute the hash.
async fn make_data_tempfile(
    data: Box<dyn AsyncRead + Send + Unpin>,
) -> anyhow::Result<(String, File)> {
    let mut file = File::from_std(
        task::spawn_blocking(tempfile)
            .await
            .map_err(anyhow::Error::from)??,
    );
    let mut hash = Sha256::new();
    let mut reader = BufReader::new(data);
    loop {
        reader.fill_buf().await?;
        let buf = reader.buffer();
        if buf.is_empty() {
            break;
        }
        hash.update(buf);
        file.write_all(buf).await?;
        reader.consume(buf.len());
    }
    let hash = format!("{:x}", hash.finalize());
    file.seek(SeekFrom::Start(0)).await?;
    Ok((hash, file))
}

fn check_range(range: Option<(u64, u64)>) -> Result<(), BlobnetError> {
    if let Some((start, end)) = range {
        if start >= end {
            return Err(anyhow!("invalid range: start >= end").into());
        }
    }
    Ok(())
}
