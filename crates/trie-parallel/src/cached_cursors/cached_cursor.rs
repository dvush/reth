use ahash::AHashMap;
use reth_primitives::{Account, StorageEntry, B256};
use reth_trie::hashed_cursor::{HashedAccountCursor, HashedCursorFactory, HashedStorageCursor};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct HashedCursorCache {
    account_cursor_cache: Arc<Mutex<AccountCursorCache>>,
    storage_cursor_cache: Arc<Mutex<HashedStorageCursorCache>>,
}

impl HashedCursorCache {
    pub fn size(&self) -> usize {
        let cache = self.account_cursor_cache.lock().unwrap();
        cache.seek_cache.len()
            + cache
            .next_cache
            .iter()
            .map(|(_, v)| v.values.len())
            .sum::<usize>()
            + self
            .storage_cursor_cache
            .lock()
            .unwrap()
            .seek_cache
            .iter()
            .map(|(_, v)| v.values.len())
            .sum::<usize>()
            + self
            .storage_cursor_cache
            .lock()
            .unwrap()
            .empty_storage
            .len()
    }
}

impl Default for HashedCursorCache {
    fn default() -> Self {
        Self {
            account_cursor_cache: Arc::new(Mutex::new(Default::default())),
            storage_cursor_cache: Arc::new(Mutex::new(Default::default())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedHashedCursorFactory<F> {
    factory: F,
    hashed_cursor_cache: HashedCursorCache,
}

impl<F: Clone> CachedHashedCursorFactory<F> {
    pub fn from_cache(cache: HashedCursorCache, factory: F) -> Self {
        Self {
            factory,
            hashed_cursor_cache: cache,
        }
    }
}

impl<F: HashedCursorFactory + Clone> HashedCursorFactory for CachedHashedCursorFactory<F> {
    type AccountCursor = CachedHashedAccountCursor<F::AccountCursor>;

    type StorageCursor = CachedHashedStorageCursor<F::StorageCursor>;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, reth_db::DatabaseError> {
        Ok(CachedHashedAccountCursor::new(
            self.factory.hashed_account_cursor()?,
            Arc::clone(&self.hashed_cursor_cache.account_cursor_cache),
        ))
    }

    fn hashed_storage_cursor(&self) -> Result<Self::StorageCursor, reth_db::DatabaseError> {
        Ok(CachedHashedStorageCursor::new(
            self.factory.hashed_storage_cursor()?,
            Arc::clone(&self.hashed_cursor_cache.storage_cursor_cache),
        ))
    }
}

#[derive(Debug)]
enum AccountCursorPos {
    Uninit,
    Seek(B256),
    Next(B256, usize),
}

#[derive(Debug)]
pub struct CachedHashedAccountCursor<C> {
    cursor: C,
    underlying_cursor_last_key: Option<B256>,
    cursor_cache: Arc<Mutex<AccountCursorCache>>,
    position: AccountCursorPos,
    last_value: Option<(B256, Account)>,
}

impl<C> CachedHashedAccountCursor<C> {
    fn new(cursor: C, cursor_cache: Arc<Mutex<AccountCursorCache>>) -> Self {
        Self {
            cursor,
            underlying_cursor_last_key: None,
            cursor_cache,
            position: AccountCursorPos::Uninit,
            last_value: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct NextAccountCacheEntry {
    terminated_size: Option<usize>,
    values: Vec<(B256, Account)>,
}

#[derive(Debug, Clone, Default)]
struct AccountCursorCache {
    seek_cache: AHashMap<B256, Option<(B256, Account)>>,
    next_cache: AHashMap<B256, NextAccountCacheEntry>,
}

impl<C: HashedAccountCursor> HashedAccountCursor for CachedHashedAccountCursor<C> {
    fn seek(&mut self, key: B256) -> Result<Option<(B256, Account)>, reth_db::DatabaseError> {
        self.position = AccountCursorPos::Seek(key);

        let mut cache = self.cursor_cache.lock().unwrap();
        if let Some(val) = cache.seek_cache.get(&key) {
            self.last_value = *val;
            return Ok(*val);
        }

        let val = self.cursor.seek(key)?;
        self.underlying_cursor_last_key = val.as_ref().map(|(k, _)| *k);

        cache.seek_cache.insert(key, val);

        self.last_value = val;
        Ok(val)
    }

    fn next(&mut self) -> Result<Option<(B256, Account)>, reth_db::DatabaseError> {
        // get position in the next cache
        let (key, index) = match self.position {
            AccountCursorPos::Uninit => unreachable!("next called before seek"),
            AccountCursorPos::Seek(key) => (key, 0),
            AccountCursorPos::Next(key, index) => (key, index + 1),
        };

        self.position = AccountCursorPos::Next(key, index);

        let mut cache = self.cursor_cache.lock().unwrap();
        // see if we have it in a cache
        let cache_entry = cache.next_cache.entry(key).or_default();

        // see if value is in the cache
        if let Some(val) = cache_entry.values.get(index) {
            self.last_value = Some(*val);
            return Ok(Some(*val));
        }
        // see if we should return None
        if let Some(size) = cache_entry.terminated_size {
            if index >= size {
                self.last_value = None;
                return Ok(None);
            }
        }

        // we need to point cursor to the last value in the cache
        let last_key = if let Some((last_key, _)) = cache_entry.values.last() {
            *last_key
        } else {
            key
        };

        if self.underlying_cursor_last_key != Some(last_key) {
            let val = self.cursor.seek(last_key)?;
            self.underlying_cursor_last_key = val.as_ref().map(|(k, _)| *k);
        }

        let next_value = self.cursor.next()?;
        self.underlying_cursor_last_key = next_value.as_ref().map(|(k, _)| *k);

        if let Some((next_key, next_value)) = &next_value {
            cache_entry.values.push((*next_key, *next_value));
            self.last_value = Some((*next_key, *next_value));
            Ok(Some((*next_key, *next_value)))
        } else {
            cache_entry.terminated_size = Some(cache_entry.values.len());
            self.last_value = None;
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, Default)]
struct NextStorageCacheEntry {
    terminated_size: Option<usize>,
    values: Vec<StorageEntry>,
}

#[derive(Debug, Clone, Default)]
struct HashedStorageCursorCache {
    empty_storage: AHashMap<B256, bool>,
    seek_cache: AHashMap<(B256, B256), NextStorageCacheEntry>,
}

#[derive(Debug, Clone, Copy)]
enum StorageCursorPos {
    Uninit,
    Seek((B256, B256), usize),
}

#[derive(Debug, Clone)]
pub struct CachedHashedStorageCursor<C> {
    cursor: C,
    cursor_cache: Arc<Mutex<HashedStorageCursorCache>>,
    cursor_pos: StorageCursorPos,
    position: StorageCursorPos,
}

impl<C> CachedHashedStorageCursor<C> {
    fn new(cursor: C, cursor_cache: Arc<Mutex<HashedStorageCursorCache>>) -> Self {
        Self {
            cursor,
            cursor_cache,
            cursor_pos: StorageCursorPos::Uninit,
            position: StorageCursorPos::Uninit,
        }
    }
}

impl<C: HashedStorageCursor> HashedStorageCursor for CachedHashedStorageCursor<C> {
    fn is_storage_empty(&mut self, key: B256) -> Result<bool, reth_db::DatabaseError> {
        let mut cache = self.cursor_cache.lock().unwrap();
        if let Some(val) = cache.empty_storage.get(&key) {
            return Ok(*val);
        }

        let val = self.cursor.is_storage_empty(key)?;
        cache.empty_storage.insert(key, val);

        Ok(val)
    }

    fn seek(
        &mut self,
        key: B256,
        subkey: B256,
    ) -> Result<Option<StorageEntry>, reth_db::DatabaseError> {
        let mut cache = self.cursor_cache.lock().unwrap();
        let key = (key, subkey);
        self.position = StorageCursorPos::Seek(key, 0);
        let entry = cache.seek_cache.entry(key).or_default();
        if let Some(val) = entry.values.first() {
            return Ok(Some(*val));
        }

        if entry.terminated_size == Some(0) {
            return Ok(None);
        }

        let val = self.cursor.seek(key.0, key.1)?;
        self.cursor_pos = StorageCursorPos::Seek(key, 0);
        if let Some(val) = &val {
            entry.values.push(*val);
        } else {
            entry.terminated_size = Some(0);
        }

        Ok(val)
    }

    fn next(&mut self) -> Result<Option<StorageEntry>, reth_db::DatabaseError> {
        let (key, index) = match self.position {
            StorageCursorPos::Uninit => unreachable!("next called before seek"),
            StorageCursorPos::Seek(key, next) => (key, next + 1),
        };

        self.position = StorageCursorPos::Seek(key, index);

        let mut cache = self.cursor_cache.lock().unwrap();
        let cache_entry = cache.seek_cache.entry(key).or_default();

        if let Some(val) = cache_entry.values.get(index) {
            return Ok(Some(*val));
        }

        if let Some(size) = cache_entry.terminated_size {
            if index >= size {
                return Ok(None);
            }
        }

        // its not in cache
        // see if we need to make seek first
        let current_key = match self.cursor_pos {
            StorageCursorPos::Seek(current_key, _) => Some(current_key),
            StorageCursorPos::Uninit => None,
        };
        if current_key != Some(key) {
            let val = self.cursor.seek(key.0, key.1)?;
            self.cursor_pos = StorageCursorPos::Seek(key, 0);
            if let Some(val) = &val {
                cache_entry.values.push(*val);
            } else {
                cache_entry.terminated_size = Some(0);
            }
        }

        let mut current_index = match self.cursor_pos {
            StorageCursorPos::Seek(_, index) => index,
            _ => unreachable!(),
        };

        while current_index < index {
            current_index += 1;
            let val = self.cursor.next()?;
            self.cursor_pos = StorageCursorPos::Seek(key, current_index);
            if let Some(val) = val {
                cache_entry.values.push(val);
            } else {
                cache_entry.terminated_size = Some(cache_entry.values.len());
                return Ok(None);
            }
        }

        Ok(cache_entry.values.get(index).cloned())
    }
}

#[cfg(test)]
mod test {
    use ahash::HashSet;
    use super::*;
    use reth_primitives::alloy_primitives::fixed_bytes;
    use reth_trie::hashed_cursor::HashedStorageCursor;

    #[derive(Debug, Clone)]
    struct TestCursorFactory {
        account_data: Vec<(B256, Account)>,
        storage_data: Vec<(B256, B256, StorageEntry)>,
        empty_storage: HashSet<B256>,
    }

    impl HashedCursorFactory for TestCursorFactory {
        type AccountCursor = TestAccountCursor;

        type StorageCursor = TestStorageCursor;

        fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, reth_db::DatabaseError> {
            Ok(TestAccountCursor {
                idx: 0,
                data: self.account_data.clone(),
                seeks: 0,
                nexts: 0,
            })
        }

        fn hashed_storage_cursor(&self) -> Result<Self::StorageCursor, reth_db::DatabaseError> {
            Ok(TestStorageCursor {
                idx: 0,
                data: self.storage_data.clone(),
                empty_storage: self.empty_storage.clone(),
                seeks: 0,
                nexts: 0,
                empty_storage_calls: 0,
            })
        }
    }

    #[derive(Debug, Clone)]
    struct TestAccountCursor {
        idx: usize,
        data: Vec<(B256, Account)>,
        seeks: usize,
        nexts: usize,
    }

    impl HashedAccountCursor for TestAccountCursor {
        fn seek(&mut self, key: B256) -> Result<Option<(B256, Account)>, reth_db::DatabaseError> {
            self.seeks += 1;
            self.idx = self
                .data
                .iter()
                .position(|(k, _)| *k == key)
                .unwrap_or(self.data.len());
            Ok(self.data.iter().find(|(k, _)| *k == key).cloned())
        }

        fn next(&mut self) -> Result<Option<(B256, Account)>, reth_db::DatabaseError> {
            self.nexts += 1;
            if self.idx >= self.data.len() {
                return Ok(None);
            }
            self.idx += 1;
            Ok(self.data.get(self.idx).cloned())
        }
    }

    #[derive(Debug, Clone)]
    struct TestStorageCursor {
        idx: usize,
        data: Vec<(B256, B256, StorageEntry)>,
        empty_storage: HashSet<B256>,
        seeks: usize,
        nexts: usize,
        empty_storage_calls: usize,
    }

    impl HashedStorageCursor for TestStorageCursor {
        fn is_storage_empty(&mut self, key: B256) -> Result<bool, reth_db::DatabaseError> {
            self.empty_storage_calls += 1;
            Ok(self.empty_storage.contains(&key))
        }

        fn seek(
            &mut self,
            key: B256,
            subkey: B256,
        ) -> Result<Option<StorageEntry>, reth_db::DatabaseError> {
            self.seeks += 1;
            self.idx = self
                .data
                .iter()
                .position(|(k, sk, _)| *k == key && *sk == subkey)
                .unwrap_or(self.data.len());
            Ok(self
                .data
                .iter()
                .find(|(k, sk, _)| *k == key && *sk == subkey)
                .map(|(_, _, v)| v)
                .cloned())
        }

        fn next(&mut self) -> Result<Option<StorageEntry>, reth_db::DatabaseError> {
            self.nexts += 1;
            if self.idx >= self.data.len() {
                return Ok(None);
            }
            self.idx += 1;
            Ok(self.data.get(self.idx).map(|(_, _, v)| v).cloned())
        }
    }

    fn test_cursor_factory() -> TestCursorFactory {
        TestCursorFactory {
            account_data: vec![
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                    Account {
                        nonce: 0,
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    Account {
                        nonce: 1,
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000002"
                    ),
                    Account {
                        nonce: 2,
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000003"
                    ),
                    Account {
                        nonce: 3,
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000004"
                    ),
                    Account {
                        nonce: 4,
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000005"
                    ),
                    Account {
                        nonce: 5,
                        ..Default::default()
                    },
                ),
            ],
            storage_data: vec![
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000011"
                    ),
                    StorageEntry {
                        key: B256::random(),
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000002"
                    ),
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000022"
                    ),
                    StorageEntry {
                        key: B256::random(),
                        ..Default::default()
                    },
                ),
                (
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000003"
                    ),
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000033"
                    ),
                    StorageEntry {
                        key: B256::random(),
                        ..Default::default()
                    },
                ),
            ],
            empty_storage: vec![fixed_bytes!(
                "1111111111111111111111111111111111111111111111111111111111111111"
            )]
                .into_iter()
                .collect(),
        }
    }

    fn get_account_cursor_values<C: HashedAccountCursor>(
        cursor: &mut C,
    ) -> Vec<Option<(B256, Account)>> {
        let mut values = Vec::new();
        values.push(
            cursor
                .seek(fixed_bytes!(
                    "0000000000000000000000000000000000000000000000000000000000000001"
                ))
                .unwrap(),
        );
        values.push(cursor.next().unwrap());
        values.push(cursor.next().unwrap());
        values.push(
            cursor
                .seek(fixed_bytes!(
                    "0000000000000000000000000000000000000000000000000000000000000002"
                ))
                .unwrap(),
        );
        values.push(cursor.next().unwrap());
        values.push(cursor.next().unwrap());
        values.push(cursor.next().unwrap());
        values.push(cursor.next().unwrap());
        values
    }

    fn get_storage_cursor_values<C: HashedStorageCursor>(
        cursor: &mut C,
    ) -> Vec<Option<StorageEntry>> {
        let mut values = Vec::new();
        values.push(
            cursor
                .seek(
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000002"
                    ),
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000022"
                    ),
                )
                .unwrap(),
        );
        values.push(cursor.next().unwrap());
        values.push(cursor.next().unwrap());
        values.push(
            cursor
                .seek(
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    fixed_bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000011"
                    ),
                )
                .unwrap(),
        );
        values.push(cursor.next().unwrap());
        values
    }

    #[test]
    fn test_cached_account_cursor() {
        let test_cursor_factory = test_cursor_factory();
        let cursor = test_cursor_factory.hashed_account_cursor().unwrap();

        let mut reference_cursor = cursor.clone();
        let reference_values = get_account_cursor_values(&mut reference_cursor);
        let (reference_seeks, reference_nexts) = (reference_cursor.seeks, reference_cursor.nexts);

        let cache = HashedCursorCache::default();
        let mut cached_cursor =
            CachedHashedAccountCursor::new(cursor.clone(), Arc::clone(&cache.account_cursor_cache));
        let cached_cursor_values = get_account_cursor_values(&mut cached_cursor);
        assert_eq!(cached_cursor_values, reference_values);
        assert_eq!(
            (cached_cursor.cursor.seeks, cached_cursor.cursor.nexts),
            (reference_seeks, reference_nexts)
        );

        let mut cached_cursor =
            CachedHashedAccountCursor::new(cursor.clone(), Arc::clone(&cache.account_cursor_cache));
        let cached_cursor_values = get_account_cursor_values(&mut cached_cursor);
        assert_eq!(cached_cursor_values, reference_values);
        assert_eq!(
            (cached_cursor.cursor.seeks, cached_cursor.cursor.nexts),
            (0, 0)
        );
    }

    #[test]
    fn test_cached_storage_cursor() {
        let test_cursor_factory = test_cursor_factory();
        let cursor = test_cursor_factory.hashed_storage_cursor().unwrap();

        let empty_storage_key =
            fixed_bytes!("1111111111111111111111111111111111111111111111111111111111111111");

        let mut reference_cursor = cursor.clone();
        let reference_values = get_storage_cursor_values(&mut reference_cursor);
        let reference_is_account_empty = reference_cursor
            .is_storage_empty(empty_storage_key)
            .unwrap();
        let (reference_seeks, reference_nexts, reference_empty_calls) = (
            reference_cursor.seeks,
            reference_cursor.nexts,
            reference_cursor.empty_storage_calls,
        );

        let cache = HashedCursorCache::default();
        let mut cached_cursor =
            CachedHashedStorageCursor::new(cursor.clone(), Arc::clone(&cache.storage_cursor_cache));
        let is_account_empty = cached_cursor.is_storage_empty(empty_storage_key).unwrap();
        assert_eq!(is_account_empty, reference_is_account_empty);
        let cached_cursor_values = get_storage_cursor_values(&mut cached_cursor);
        assert_eq!(cached_cursor_values, reference_values);
        assert_eq!(
            (
                cached_cursor.cursor.seeks,
                cached_cursor.cursor.nexts,
                cached_cursor.cursor.empty_storage_calls
            ),
            (reference_seeks, reference_nexts, reference_empty_calls)
        );

        let mut cached_cursor =
            CachedHashedStorageCursor::new(cursor.clone(), Arc::clone(&cache.storage_cursor_cache));
        let is_account_empty = cached_cursor.is_storage_empty(empty_storage_key).unwrap();
        assert_eq!(is_account_empty, reference_is_account_empty);
        let cached_cursor_values = get_storage_cursor_values(&mut cached_cursor);
        assert_eq!(cached_cursor_values, reference_values);
        assert_eq!(
            (
                cached_cursor.cursor.seeks,
                cached_cursor.cursor.nexts,
                cached_cursor.cursor.empty_storage_calls
            ),
            (0, 0, 0)
        );
    }

    #[test]
    fn test_cached_cursor_factory() {
        let test_cursor_factory = test_cursor_factory();
        let cache = HashedCursorCache::default();
        let cursor_factory =
            CachedHashedCursorFactory::from_cache(cache.clone(), test_cursor_factory.clone());

        {
            let mut cursor = cursor_factory.hashed_account_cursor().unwrap();
            get_account_cursor_values(&mut cursor);
        }
        dbg!(&cache);
    }
}
