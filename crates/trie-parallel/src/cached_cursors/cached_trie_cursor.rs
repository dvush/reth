use ahash::AHashMap;
use reth_primitives::{
    trie::{BranchNodeCompact, Nibbles},
    B256,
};
use reth_interfaces::db::DatabaseError;
use reth_trie::{
    trie_cursor::{TrieCursor, TrieCursorFactory},
    updates::TrieKey,
};
use std::sync::{Arc, Mutex};

type AccountCursorCache = Arc<Mutex<TrieCursorCache>>;
type TrieCursorCacheSubKey = Arc<Mutex<TrieCursorCache>>;
type StorageCursorCache = Arc<Mutex<AHashMap<B256, TrieCursorCacheSubKey>>>;

#[derive(Debug, Clone)]
pub struct TrieCursorsCaches {
    account_cache: AccountCursorCache,
    storage_cache: StorageCursorCache,
}

impl Default for TrieCursorsCaches {
    fn default() -> Self {
        Self {
            account_cache: Arc::new(Mutex::new(TrieCursorCache::default())),
            storage_cache: Arc::new(Mutex::new(AHashMap::default())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedTrieCursorFactory<F> {
    factory: F,
    cache: TrieCursorsCaches,
}

impl<F: TrieCursorFactory + Clone> CachedTrieCursorFactory<F> {
    pub fn new(factory: F, cache: TrieCursorsCaches) -> Self {
        Self { factory, cache }
    }
}

impl<F: TrieCursorFactory> TrieCursorFactory for CachedTrieCursorFactory<F> {
    fn account_trie_cursor(&self) -> Result<Box<dyn TrieCursor + '_>, DatabaseError> {
        let cursor = self.factory.account_trie_cursor()?;
        Ok(Box::new(CachedTrieCursor::new(
            cursor,
            self.cache.account_cache.clone(),
        )))
    }

    fn storage_tries_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Box<dyn TrieCursor + '_>, DatabaseError> {
        let cursor = self.factory.storage_tries_cursor(hashed_address)?;
        let mut storage_cache = self.cache.storage_cache.lock().unwrap();
        let cache = storage_cache
            .entry(hashed_address)
            .or_insert_with(|| Arc::new(Mutex::new(TrieCursorCache::default())))
            .clone();
        Ok(Box::new(CachedTrieCursor::new(cursor, cache)))
    }
}

#[derive(Debug, Clone)]
pub struct TrieCursorCache {
    seek: AHashMap<Nibbles, Option<(Nibbles, BranchNodeCompact)>>,
    seek_exact: AHashMap<Nibbles, Option<(Nibbles, BranchNodeCompact)>>,
    current: AHashMap<SeekArgs, Option<TrieKey>>,
}

impl Default for TrieCursorCache {
    fn default() -> Self {
        Self {
            seek: AHashMap::new(),
            seek_exact: AHashMap::new(),
            current: AHashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SeekArgs {
    None,
    SeekExact(Nibbles),
    Seek(Nibbles),
}

impl SeekArgs {
    fn into_key(self) -> Option<Nibbles> {
        match self {
            SeekArgs::SeekExact(key) => Some(key),
            SeekArgs::Seek(key) => Some(key),
            SeekArgs::None => None,
        }
    }
}

#[derive(Debug)]
pub struct CachedTrieCursor<C: TrieCursor> {
    cursor: C,
    cursor_last_seek: SeekArgs,
    last_seek: SeekArgs,
    cache: Arc<Mutex<TrieCursorCache>>,
}

impl<C: TrieCursor> CachedTrieCursor<C> {
    pub fn new(cursor: C, cache: Arc<Mutex<TrieCursorCache>>) -> Self {
        Self {
            cursor,
            cursor_last_seek: SeekArgs::None,
            last_seek: SeekArgs::None,
            cache,
        }
    }
}

impl<C: TrieCursor> TrieCursor for CachedTrieCursor<C> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.last_seek = SeekArgs::SeekExact(key.clone());
        let mut cache = self.cache.lock().unwrap();
        if let Some(value) = cache.seek_exact.get(&key) {
            return Ok(value.clone());
        }

        let value = self.cursor.seek_exact(key.clone())?;
        self.cursor_last_seek = SeekArgs::SeekExact(key.clone());
        cache.seek_exact.insert(key, value.clone());
        Ok(value)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.last_seek = SeekArgs::Seek(key.clone());
        let mut cache = self.cache.lock().unwrap();
        if let Some(value) = cache.seek.get(&key) {
            return Ok(value.clone());
        }

        let value = self.cursor.seek(key.clone())?;
        self.cursor_last_seek = SeekArgs::Seek(key.clone());
        cache.seek.insert(key, value.clone());
        Ok(value)
    }

    fn current(&mut self) -> Result<Option<TrieKey>, DatabaseError> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(value) = cache.current.get(&self.last_seek) {
            return Ok(value.clone());
        }

        if self.cursor_last_seek != self.last_seek {
            self.cursor.seek(
                self.last_seek
                    .clone()
                    .into_key()
                    .expect("current called before seek"),
            )?;
            self.cursor_last_seek = self.last_seek.clone();
        }

        let value = self.cursor.current()?;
        cache.current.insert(self.last_seek.clone(), value.clone());
        Ok(value)
    }
}
