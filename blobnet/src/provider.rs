//! Configurable, async storage providers for blob access.

use std::collections::HashMap;
use std::future::Future;
use std::io::{self, Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use auto_impl::auto_impl;
use aws_sdk_s3::{
    error::{GetObjectErrorKind, HeadObjectErrorKind},
    types::{ByteStream, SdkError},
};
use hashlink::LinkedHashMap;
use hyper::{body::Bytes, client::connect::Connect};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tempfile::tempfile;
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::{task, time};
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

use crate::client::FileClient;
use crate::utils::{atomic_copy, hash_path, stream_body};
use crate::{read_to_vec, Error, ReadStream};

/// Specifies a storage backend for the blobnet service.
///
/// Each method returns an error only when some operational problem occurs, such
/// as in I/O or communication. Retries should be handled internally by the
/// function since each provider has different failure modes.
///
/// This trait was designed to support flexible combinators that can be used to
/// add caching or fallbacks to providers.
#[async_trait]
#[auto_impl(&, Box, Arc)]
pub trait Provider: Send + Sync {
    /// Check if a file exists and returns its size in bytes.
    async fn head(&self, hash: &str) -> Result<u64, Error>;

    /// Returns the data from the file at the given path.
    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error>;

    /// Adds a binary blob to storage, returning its hash.
    ///
    /// This function is not as latency-sensitive as the others, caring more
    /// about throughput. It may take two passes over the data.
    async fn put(&self, data: ReadStream) -> Result<String, Error>;
}

/// A provider that stores blobs in memory, only used for testing.
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
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        let data = self.data.read();
        let bytes = data.get(hash).ok_or(Error::NotFound)?;
        Ok(bytes.len() as u64)
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error> {
        check_range(range)?;
        let data = self.data.read();
        let mut bytes = match data.get(hash) {
            Some(bytes) => bytes.clone(),
            None => return Err(Error::NotFound),
        };
        if let Some((start, end)) = range {
            if start > bytes.len() as u64 {
                return Err(Error::BadRange);
            }
            bytes = bytes.slice(start as usize..bytes.len().min(end as usize));
        }
        Ok(Box::pin(Cursor::new(bytes)))
    }

    async fn put(&self, mut data: ReadStream) -> Result<String, Error> {
        let mut buf = Vec::new();
        data.read_to_end(&mut buf).await?;
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
    async fn head(&self, hash: &str) -> Result<u64, Error> {
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
                Err(Error::NotFound)
            }
            Err(err) => Err(Error::Internal(err.into())),
        }
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error> {
        check_range(range)?;

        if matches!(range, Some((s, e)) if s == e) {
            // Special case: The range has length 0, and S3 doesn't support
            // zero-length ranges directly, so we need a different request.
            let len = self.head(hash).await?;
            if range.unwrap().0 > len {
                return Err(Error::BadRange);
            } else {
                return Ok(empty_stream());
            }
        }

        let key = hash_path(hash)?;
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .set_range(range.map(|(start, end)| format!("bytes={}-{}", start, end - 1)))
            .send()
            .await;

        match result {
            Ok(resp) => Ok(Box::pin(resp.body.into_async_read())),
            Err(SdkError::ServiceError { err, .. })
                if matches!(err.kind, GetObjectErrorKind::NoSuchKey(_)) =>
            {
                Err(Error::NotFound)
            }
            // InvalidRange isn't supported on the `GetObjectErrorKind` enum.
            Err(SdkError::ServiceError { err, .. }) if err.code() == Some("InvalidRange") => {
                // Edge case: S3 throws errors if the start of the range is at exactly the
                // length of the file, but we want to support this use case.
                //
                // Unfortunately there's no way to get the "<ActualObjectSize>" XML property
                // from the error response in the current SDK, so we make a second request.
                let len = self.head(hash).await?;
                match range {
                    Some((s, _)) if s == len => Ok(empty_stream()),
                    _ => Err(Error::BadRange),
                }
            }
            Err(err) => Err(Error::Internal(err.into())),
        }
    }

    async fn put(&self, data: ReadStream) -> Result<String, Error> {
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
            .checksum_sha256(base64::encode(hex::decode(&hash).unwrap()))
            .body(body)
            .send()
            .await
            .map_err(anyhow::Error::from)?;
        Ok(hash)
    }
}

/// A provider that stores blobs in a local directory.
///
/// This is especially useful when targeting network file systems mounts.
pub struct LocalDir {
    dir: PathBuf,
}

impl LocalDir {
    /// Creates a new local directory provider.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            dir: path.as_ref().to_owned(),
        }
    }
}

#[async_trait]
impl Provider for LocalDir {
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        let key = hash_path(hash)?;
        let path = self.dir.join(key);
        match fs::metadata(&path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(Error::NotFound),
            Err(err) => Err(err.into()),
        }
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error> {
        check_range(range)?;
        let key = hash_path(hash)?;
        let path = self.dir.join(key);
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Err(Error::NotFound),
            Err(err) => return Err(err.into()),
        };
        if let Some((start, end)) = range {
            let len = file.metadata().await?.len();
            if start > len {
                Err(Error::BadRange)
            } else {
                file.seek(SeekFrom::Start(start)).await?;
                Ok(Box::pin(file.take(end - start)))
            }
        } else {
            Ok(Box::pin(file))
        }
    }

    async fn put(&self, data: ReadStream) -> Result<String, Error> {
        let (hash, file) = make_data_tempfile(data).await?;
        let file = file.into_std().await;
        let key = hash_path(&hash)?;
        let path = self.dir.join(key);
        task::spawn_blocking(move || atomic_copy(file, path))
            .await
            .map_err(anyhow::Error::from)??;
        Ok(hash)
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
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        self.client.head(hash).await
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error> {
        self.client.get(hash, range).await
    }

    async fn put(&self, data: ReadStream) -> Result<String, Error> {
        let (_, file) = make_data_tempfile(data).await?;
        self.client
            .put(|| async { Ok(stream_body(Box::pin(file.try_clone().await?))) })
            .await
    }
}

/// Stream data from a source into a temporary file and compute the hash.
async fn make_data_tempfile(data: ReadStream) -> anyhow::Result<(String, File)> {
    let mut file = File::from_std(task::spawn_blocking(tempfile).await??);
    let mut hash = Sha256::new();
    let mut reader = BufReader::new(data);
    loop {
        let size = {
            let buf = reader.fill_buf().await?;
            if buf.is_empty() {
                break;
            }
            hash.update(buf);
            file.write_all(buf).await?;
            buf.len()
        };
        reader.consume(size);
    }
    let hash = format!("{:x}", hash.finalize());
    file.seek(SeekFrom::Start(0)).await?;
    Ok((hash, file))
}

fn check_range(range: Option<(u64, u64)>) -> Result<(), Error> {
    match range {
        Some((start, end)) if start > end => Err(Error::BadRange),
        _ => Ok(()),
    }
}

fn empty_stream() -> ReadStream {
    Box::pin(Cursor::new(Bytes::new()))
}

// A pair of providers is also a provider, acting as a fallback.
//
// This is useful for gradually migrating between two providers without
// downtime. Note that PUT requests are only sent to the primary provider.
#[async_trait]
impl<P1: Provider, P2: Provider> Provider for (P1, P2) {
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        match self.0.head(hash).await {
            Ok(res) => Ok(res),
            Err(_) => self.1.head(hash).await,
        }
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error> {
        match self.0.get(hash, range).await {
            Ok(res) => Ok(res),
            Err(_) => self.1.get(hash, range).await,
        }
    }

    async fn put(&self, data: ReadStream) -> Result<String, Error> {
        self.0.put(data).await
    }
}

/// A provider wrapper that caches data locally.
pub struct Cached<P> {
    state: Arc<CachedState<P>>,
}

/// Constant cost associated with every entry in the page cache.
const PAGE_CACHE_ENTRY_COST: u64 = 80;

/// An in-memory, two-stage LRU based page cache.
struct PageCache {
    /// A mapping from `(hash, offset)` pairs to shared references to data.
    mapping: LinkedHashMap<(String, u64), Bytes>,
    /// The total cached pages' size, plus a fixed [`PAGE_CACHE_ENTRY_COST`].
    total_cost: u64,
    /// The maximum cost size of the page cache in bytes.
    total_capacity: u64,
}

impl PageCache {
    /// Insert an entry into the page cache, with LRU eviction.
    fn insert(&mut self, hash: String, n: u64, bytes: Bytes) {
        use hashlink::linked_hash_map::Entry;

        match self.mapping.entry((hash, n)) {
            Entry::Occupied(mut o) => o.to_back(),
            Entry::Vacant(v) => {
                v.insert(bytes.clone());
                self.total_cost += bytes.len() as u64 + PAGE_CACHE_ENTRY_COST;
                while self.total_cost > self.total_capacity {
                    // This should never panic because of a data structure invariant. If we reached
                    // this line, the total cost in the page cache must be nonzero, so there must be
                    // still pages in the mapping.
                    let (_, bytes) = self.mapping.pop_front().expect("cache with cost items");
                    self.total_cost -= bytes.len() as u64 + PAGE_CACHE_ENTRY_COST;
                }
            }
        }
    }

    /// Get an entry from the page cache, with LRU eviction.
    fn get(&mut self, hash: String, n: u64) -> Option<Bytes> {
        use hashlink::linked_hash_map::Entry;

        match self.mapping.entry((hash, n)) {
            Entry::Occupied(mut o) => {
                o.to_back();
                Some(o.get().clone())
            }
            Entry::Vacant(_) => None,
        }
    }
}

impl Default for PageCache {
    fn default() -> Self {
        Self {
            mapping: LinkedHashMap::new(),
            total_cost: 0,
            total_capacity: 1 << 25, // 32 MiB in-memory page cache
        }
    }
}

struct CachedState<P> {
    inner: P,
    page_cache: Mutex<PageCache>,
    dir: PathBuf,
    pagesize: u64,
}

impl<P: 'static> Cached<P> {
    /// Create a new cache wrapper for the given provider.
    ///
    /// Set the page size in bytes for cached chunks, as well as the directory
    /// where fragments should be stored.
    pub fn new(inner: P, dir: impl AsRef<Path>, pagesize: u64) -> Self {
        assert!(pagesize >= 4096, "pagesize must be at least 4096");
        Self {
            state: Arc::new(CachedState {
                inner,
                page_cache: Default::default(),
                dir: dir.as_ref().to_owned(), // File system cache
                pagesize,
            }),
        }
    }

    /// A background process that periodically cleans the cache directory.
    ///
    /// Since the cache directory is limited in size but local to the machine,
    /// it is acceptable to delete files from this folder at any time.
    /// Therefore, we can simply remove 1/(256^2) of all files at an
    /// interval of 60 seconds.
    ///
    /// Doing the math, it would take (256^2) / 60 / 24 = ~46 days on average to
    /// expire any given file from the disk cache directory.
    pub fn cleaner(&self) -> impl Future<Output = ()> {
        let state = Arc::clone(&self.state);
        async move { state.cleaner().await }
    }
}

impl<P> CachedState<P> {
    /// Run a function with both file system and memory caches.
    ///
    /// The first cache page of each hash stores HEAD metadata. After that,
    /// index `n` stores the byte range from `(n - 1) * pagesize` to `n *
    /// pagesize`.
    async fn with_cache<F, Out>(&self, hash: String, n: u64, func: F) -> Result<Bytes, Error>
    where
        F: FnOnce() -> Out,
        Out: Future<Output = Result<Bytes, Error>>,
    {
        if let Some(bytes) = self.page_cache.lock().get(hash.clone(), n) {
            return Ok(bytes);
        }

        let key = hash_path(&hash)?;
        let path = self.dir.join(format!("{key}/{n}"));
        if let Ok(data) = fs::read(&path).await {
            let bytes = Bytes::from(data);
            self.page_cache
                .lock()
                .insert(hash.clone(), n, bytes.clone());
            return Ok(bytes);
        }

        let bytes = func().await?;
        let read_buf = Cursor::new(bytes.clone());
        task::spawn_blocking(move || atomic_copy(read_buf, &path))
            .await
            .map_err(anyhow::Error::from)??;
        self.page_cache.lock().insert(hash, n, bytes.clone());
        Ok(bytes)
    }

    async fn cleaner(&self) {
        const CLEAN_INTERVAL: Duration = Duration::from_secs(30);
        loop {
            time::sleep(CLEAN_INTERVAL).await;
            let prefix = fastrand::u16(..);
            let (d1, d2) = (prefix / 256, prefix % 256);
            let subfolder = self.dir.join(&format!("{d1:x}/{d2:x}"));
            if fs::metadata(&subfolder).await.is_ok() {
                println!("cleaning cache directory: {}", subfolder.display());
                let subfolder_tmp = self.dir.join(&format!("{d1:x}/.tmp-{d2:x}"));
                fs::remove_dir_all(&subfolder_tmp).await.ok();
                if fs::rename(&subfolder, &subfolder_tmp).await.is_ok() {
                    fs::remove_dir_all(&subfolder_tmp).await.ok();
                }
            }
        }
    }
}

impl<P: Provider> CachedState<P> {
    /// Read the size of a file, with caching.
    async fn get_cached_size(&self, hash: &str) -> Result<u64, Error> {
        let size = self
            .with_cache(hash.into(), 0, || async {
                let size = self.inner.head(hash).await?;
                Ok(Bytes::from_iter(size.to_le_bytes()))
            })
            .await?;
        Ok(u64::from_le_bytes(
            size.as_ref().try_into().map_err(anyhow::Error::from)?,
        ))
    }

    /// Read a chunk of data, with caching.
    async fn get_cached_chunk(&self, hash: &str, n: u64) -> Result<Bytes, Error> {
        assert!(n > 0, "chunks of file data start at 1");

        let lo = (n - 1) * self.pagesize;
        let hi = n * self.pagesize;

        self.with_cache(hash.into(), n, move || async move {
            Ok(read_to_vec(self.inner.get(hash, Some((lo, hi))).await?)
                .await?
                .into())
        })
        .await
    }
}

#[async_trait]
impl<P: Provider + 'static> Provider for Cached<P> {
    async fn head(&self, hash: &str) -> Result<u64, Error> {
        self.state.get_cached_size(hash).await
    }

    async fn get(&self, hash: &str, range: Option<(u64, u64)>) -> Result<ReadStream, Error> {
        let (start, end) = range.unwrap_or((0, u64::MAX));
        check_range(range)?;

        if start == end {
            // Special case: The range has length 0, so we can't divide it into chunks.
            let len = self.head(hash).await?;
            if start > len {
                return Err(Error::BadRange);
            } else {
                return Ok(empty_stream());
            }
        }

        let chunk_begin: u64 = 1 + start / self.state.pagesize;
        let chunk_end: u64 = 1 + (end - 1) / self.state.pagesize;
        debug_assert!(chunk_begin >= 1);
        debug_assert!(chunk_begin <= chunk_end);

        // Read the first chunk, and return BadRange if out of bounds (or NotFound if
        // non-existent). Otherwise, the range should be valid, and we can continue
        // reading until we reach the end of the requested range or get an error.
        let first_chunk = self.state.get_cached_chunk(hash, chunk_begin).await?;
        let reached_end = (first_chunk.len() as u64) < self.state.pagesize;
        let initial_offset = start - (chunk_begin - 1) * self.state.pagesize;
        if initial_offset > first_chunk.len() as u64 {
            return Err(Error::BadRange);
        }

        let first_chunk = first_chunk.slice(initial_offset as usize..);
        // If it fits in a single chunk, just return the data immediately.
        if reached_end || first_chunk.len() as u64 > end - start {
            let total_len = first_chunk.len().min((end - start) as usize);
            return Ok(Box::pin(Cursor::new(first_chunk.slice(..total_len))));
        }
        let remaining_bytes = Arc::new(Mutex::new(end - start - first_chunk.len() as u64));

        let state = Arc::clone(&self.state);
        let hash = hash.to_string();
        let stream = tokio_stream::iter(chunk_begin..=chunk_end).then(move |chunk| {
            let state = Arc::clone(&state);
            let remaining_bytes = Arc::clone(&remaining_bytes);
            let first_chunk = first_chunk.clone();
            let hash = hash.clone();

            async move {
                if chunk == chunk_begin {
                    return Ok::<_, Error>(first_chunk);
                }
                let bytes = state.get_cached_chunk(&hash, chunk).await?;
                let mut remaining_bytes = remaining_bytes.lock();
                if bytes.len() as u64 > *remaining_bytes {
                    let result = bytes.slice(..*remaining_bytes as usize);
                    *remaining_bytes = 0;
                    Ok(result)
                } else {
                    *remaining_bytes -= bytes.len() as u64;
                    Ok(bytes)
                }
            }
        });
        let stream = stream.take_while(|result| match result {
            Ok(bytes) => !bytes.is_empty(),
            Err(Error::BadRange) => false, // gracefully end the stream
            Err(_) => true,
        });
        Ok(Box::pin(StreamReader::new(stream)))
    }

    async fn put(&self, data: ReadStream) -> Result<String, Error> {
        self.state.inner.put(data).await
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use hyper::body::Bytes;

    use super::{Memory, PageCache, Provider};
    use crate::Error;

    #[test]
    fn page_cache_eviction() {
        let mut cache = PageCache::default();
        let bigpage = Bytes::from(vec![42; 1 << 21]);
        for i in 0..4096 {
            cache.insert(String::new(), i, bigpage.clone());
        }
        assert_eq!(cache.get(String::new(), 0), None);
        assert_eq!(cache.get(String::new(), 4095), Some(bigpage));
        assert!(cache.mapping.len() < 2048);
    }

    #[test]
    fn page_cache_duplicates() {
        let mut cache = PageCache::default();
        let page = Bytes::from(vec![42; 256]);
        for _ in 0..4096 {
            cache.insert(String::new(), 0, page.clone());
        }
        assert_eq!(cache.get(String::new(), 0), Some(page));
        assert!(cache.mapping.len() == 1);
    }

    #[tokio::test]
    async fn fallback_provider() {
        let p = (Memory::new(), Memory::new());
        let hash = p
            .put(Box::pin(Cursor::new(vec![42; 1 << 21])))
            .await
            .unwrap();

        assert!(matches!(p.get(&hash, None).await, Ok(_)));
        assert!(matches!(p.0.get(&hash, None).await, Ok(_)));
        assert!(matches!(p.1.get(&hash, None).await, Err(Error::NotFound)));
    }
}
