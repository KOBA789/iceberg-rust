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

//! Byte-range cache for immutable Parquet data files.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;

use crate::io::FileRead;
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct CacheKey {
    path: Arc<str>,
    file_size: u64,
    start: u64,
    end: u64,
}

/// A weighted LRU cache for byte ranges read from immutable Parquet data files.
///
/// The file size is part of the cache key as a guard against path reuse.
/// Concurrent misses for the same range are coalesced into a single read.
#[derive(Clone, Debug)]
pub struct ParquetReadCache {
    cache: Cache<CacheKey, Bytes>,
}

impl ParquetReadCache {
    /// Creates a cache with the given maximum capacity in bytes.
    pub fn new(max_capacity_bytes: u64) -> Self {
        Self {
            cache: Cache::builder()
                .weigher(|_key: &CacheKey, value: &Bytes| {
                    value.len().try_into().unwrap_or(u32::MAX)
                })
                .max_capacity(max_capacity_bytes)
                .build(),
        }
    }

    /// Invalidates every cached range.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    async fn get_or_read<Fut>(
        &self,
        path: &Arc<str>,
        file_size: u64,
        range: &Range<u64>,
        read: Fut,
    ) -> Result<Bytes>
    where
        Fut: Future<Output = Result<Bytes>>,
    {
        let key = CacheKey {
            path: Arc::clone(path),
            file_size,
            start: range.start,
            end: range.end,
        };
        self.cache
            .entry(key)
            .or_try_insert_with(read)
            .await
            .map(|entry| entry.into_value())
            .map_err(|error| {
                Error::new(ErrorKind::Unexpected, "Parquet read cache load failed")
                    .with_source(error)
            })
    }
}

pub(crate) struct CachedFileRead {
    inner: Box<dyn FileRead>,
    path: Arc<str>,
    file_size: u64,
    cache: ParquetReadCache,
}

impl CachedFileRead {
    pub(crate) fn new(
        inner: Box<dyn FileRead>,
        path: Arc<str>,
        file_size: u64,
        cache: ParquetReadCache,
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
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.cache
            .get_or_read(
                &self.path,
                self.file_size,
                &range,
                self.inner.read(range.clone()),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct CountingRead {
        calls: Arc<AtomicUsize>,
        bytes: Bytes,
    }

    #[async_trait::async_trait]
    impl FileRead for CountingRead {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn repeated_ranges_are_read_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = CachedFileRead::new(
            Box::new(CountingRead {
                calls: Arc::clone(&calls),
                bytes: Bytes::from_static(b"0123456789"),
            }),
            Arc::from("s3://bucket/data.parquet"),
            10,
            ParquetReadCache::new(1024),
        );

        assert_eq!(reader.read(2..6).await.unwrap(), "2345");
        assert_eq!(reader.read(2..6).await.unwrap(), "2345");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
