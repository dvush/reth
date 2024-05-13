mod cached_cursor;
mod cached_trie_cursor;

pub use cached_cursor::*;
pub use cached_trie_cursor::*;

#[derive(Debug, Clone, Default)]
pub struct CursorCache {
    pub hashed_cursor: HashedCursorCache,
    pub trie_cursor: TrieCursorsCaches,
}
