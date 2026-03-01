// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Byte-range cache for Parquet DataFiles.
//!
//! Iceberg DataFiles are immutable — once written, the content at a given path
//! never changes. This module exploits that property to cache byte ranges read
//! from Parquet files, eliminating redundant S3 reads across repeated queries
//! that hit the same DataFiles.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;

use crate::io::FileRead;
use crate::{Error, ErrorKind};

/// Cache key identifying a specific byte range within a Parquet DataFile.
///
/// `file_size` is included as a safety guard against hypothetical path reuse —
/// if a path were ever reused with different content, the file size would differ.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct CacheKey {
    path: Arc<str>,
    file_size: u64,
    start: u64,
    end: u64,
}

/// LRU cache for Parquet DataFile byte ranges.
///
/// Since Iceberg DataFiles are immutable, cached entries are valid indefinitely
/// (no TTL needed). Eviction is based on weighted LRU using the byte length of
/// each cached value.
///
/// Concurrent misses for the same key are deduplicated via moka's
/// `entry_by_ref().or_try_insert_with()` — only one S3 read is issued and the
/// result is shared among all waiters (single-flight dedup).
#[derive(Clone, Debug)]
pub struct ParquetReadCache {
    cache: Cache<CacheKey, Bytes>,
}

impl ParquetReadCache {
    /// Create a new cache with the given maximum capacity in bytes.
    ///
    /// The returned cache is cheaply cloneable (moka `Cache` is `Arc`-based)
    /// and can be shared across multiple `Table` instances.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key: &CacheKey, value: &Bytes| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .max_capacity(max_capacity_bytes)
            .build();
        Self { cache }
    }

    /// Invalidate all cached entries.
    ///
    /// Use this when a table is dropped and recreated at the same path —
    /// while Iceberg DataFiles are normally immutable, drop/recreate can
    /// theoretically reuse paths.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Retrieve a byte range from the cache, or load it using `read_fn`.
    ///
    /// Uses single-flight dedup: if multiple tasks request the same
    /// (path, file_size, start, end) concurrently, only one executes `read_fn`
    /// and the result is shared among all waiters.
    pub(crate) async fn get_or_read<Fut>(
        &self,
        path: &Arc<str>,
        file_size: u64,
        range: &Range<u64>,
        read_fn: Fut,
    ) -> crate::Result<Bytes>
    where
        Fut: std::future::Future<Output = crate::Result<Bytes>>,
    {
        let key = CacheKey {
            path: Arc::clone(path),
            file_size,
            start: range.start,
            end: range.end,
        };
        let entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(read_fn)
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parquet read cache load failed").with_source(e)
            })?;
        Ok(entry.into_value())
    }
}

/// [`FileRead`] wrapper that caches byte-range reads via [`ParquetReadCache`].
///
/// When `cache` is `Some`, reads are served from cache (or loaded into cache on
/// miss). When `cache` is `None`, reads are forwarded directly to the inner
/// reader. This design avoids the need for a `MaybeCached` enum — all code paths
/// use `CachedFileRead` uniformly.
pub(crate) struct CachedFileRead {
    inner: Box<dyn FileRead>,
    path: Arc<str>,
    file_size: u64,
    cache: Option<ParquetReadCache>,
}

impl CachedFileRead {
    pub(crate) fn new(
        inner: Box<dyn FileRead>,
        path: Arc<str>,
        file_size: u64,
        cache: Option<ParquetReadCache>,
    ) -> Self {
        Self {
            inner,
            path,
            file_size,
            cache,
        }
    }
}

#[async_trait::async_trait]
impl FileRead for CachedFileRead {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        if let Some(cache) = &self.cache {
            return cache
                .get_or_read(&self.path, self.file_size, &range, self.inner.read(range.clone()))
                .await;
        }
        self.inner.read(range).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock FileRead that counts how many times `read` is called.
    struct MockFileRead {
        call_count: Arc<AtomicUsize>,
        data: Bytes,
    }

    impl MockFileRead {
        fn new(data: &[u8]) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                data: Bytes::copy_from_slice(data),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl FileRead for MockFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let start = range.start as usize;
            let end = (range.end as usize).min(self.data.len());
            Ok(self.data.slice(start..end))
        }
    }

    #[tokio::test]
    async fn test_cache_hit_returns_cached_bytes() {
        let cache = ParquetReadCache::new(1024);
        let path: Arc<str> = Arc::from("s3://bucket/file.parquet");
        let data = Bytes::from_static(b"hello world");

        // Insert
        let result = cache
            .get_or_read(&path, 100, &(0..11), async { Ok(data.clone()) })
            .await
            .unwrap();
        assert_eq!(result, data);

        // Hit — the future should not be called
        let result = cache
            .get_or_read(&path, 100, &(0..11), async {
                panic!("should not be called on cache hit")
            })
            .await
            .unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_cache_miss_calls_read_fn() {
        let cache = ParquetReadCache::new(1024);
        let path: Arc<str> = Arc::from("s3://bucket/file.parquet");
        let data = Bytes::from_static(b"test data");

        let result = cache
            .get_or_read(&path, 100, &(0..9), async { Ok(data.clone()) })
            .await
            .unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_different_ranges_cached_independently() {
        let cache = ParquetReadCache::new(1024);
        let path: Arc<str> = Arc::from("s3://bucket/file.parquet");

        let r1 = cache
            .get_or_read(&path, 100, &(0..5), async {
                Ok(Bytes::from_static(b"hello"))
            })
            .await
            .unwrap();
        let r2 = cache
            .get_or_read(&path, 100, &(5..10), async {
                Ok(Bytes::from_static(b"world"))
            })
            .await
            .unwrap();

        assert_eq!(r1, Bytes::from_static(b"hello"));
        assert_eq!(r2, Bytes::from_static(b"world"));
    }

    #[tokio::test]
    async fn test_different_paths_cached_independently() {
        let cache = ParquetReadCache::new(1024);
        let path1: Arc<str> = Arc::from("s3://bucket/file1.parquet");
        let path2: Arc<str> = Arc::from("s3://bucket/file2.parquet");

        let r1 = cache
            .get_or_read(&path1, 100, &(0..5), async {
                Ok(Bytes::from_static(b"data1"))
            })
            .await
            .unwrap();
        let r2 = cache
            .get_or_read(&path2, 200, &(0..5), async {
                Ok(Bytes::from_static(b"data2"))
            })
            .await
            .unwrap();

        assert_eq!(r1, Bytes::from_static(b"data1"));
        assert_eq!(r2, Bytes::from_static(b"data2"));
    }

    #[tokio::test]
    async fn test_file_size_differentiates_cache_entries() {
        // Same path + range but different file_size should be treated as
        // different cache entries (safety guard against path reuse).
        let cache = ParquetReadCache::new(1024);
        let path: Arc<str> = Arc::from("s3://bucket/file.parquet");

        cache
            .get_or_read(&path, 100, &(0..5), async {
                Ok(Bytes::from_static(b"old__"))
            })
            .await
            .unwrap();

        // Same path+range but different file_size
        let result = cache
            .get_or_read(&path, 200, &(0..5), async {
                Ok(Bytes::from_static(b"new__"))
            })
            .await
            .unwrap();

        assert_eq!(result, Bytes::from_static(b"new__"));

        // Original entry still cached under file_size=100
        let result = cache
            .get_or_read(&path, 100, &(0..5), async {
                panic!("should hit cache for file_size=100")
            })
            .await
            .unwrap();
        assert_eq!(result, Bytes::from_static(b"old__"));
    }

    #[tokio::test]
    async fn test_cached_file_read_caches_reads() {
        let mock = MockFileRead::new(b"0123456789abcdef");
        let call_count = mock.call_count.clone();
        let cache = ParquetReadCache::new(1024);
        let reader = CachedFileRead::new(
            Box::new(mock),
            Arc::from("s3://bucket/file.parquet"),
            16,
            Some(cache),
        );

        // First read — should call inner
        let r1 = reader.read(0..5).await.unwrap();
        assert_eq!(r1, Bytes::from_static(b"01234"));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second read of same range — should hit cache
        let r2 = reader.read(0..5).await.unwrap();
        assert_eq!(r2, Bytes::from_static(b"01234"));
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // still 1
    }

    #[tokio::test]
    async fn test_cached_file_read_without_cache() {
        let mock = MockFileRead::new(b"0123456789abcdef");
        let call_count = mock.call_count.clone();
        let reader = CachedFileRead::new(
            Box::new(mock),
            Arc::from("s3://bucket/file.parquet"),
            16,
            None,
        );

        let r1 = reader.read(0..5).await.unwrap();
        assert_eq!(r1, Bytes::from_static(b"01234"));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Without cache, same range reads inner again
        let r2 = reader.read(0..5).await.unwrap();
        assert_eq!(r2, Bytes::from_static(b"01234"));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_same_range_reads_inner_once() {
        let mock = MockFileRead::new(b"0123456789abcdef");
        let call_count = mock.call_count.clone();
        let cache = ParquetReadCache::new(1024);
        let reader = CachedFileRead::new(
            Box::new(mock),
            Arc::from("s3://bucket/file.parquet"),
            16,
            Some(cache),
        );

        // Read same range 5 times
        for _ in 0..5 {
            let result = reader.read(0..5).await.unwrap();
            assert_eq!(result, Bytes::from_static(b"01234"));
        }
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
