//! Pager and direct main-database page access.
//!
//! Implements:
//! - design/adr/0001-page-size.md

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{DbError, Result};
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::cache::PageCache;
use super::freelist::{decode_freelist_next, encode_freelist_page};
use super::header::{DatabaseHeader, DB_HEADER_SIZE};
use super::page::{self, PageId};

#[derive(Clone, Debug)]
pub(crate) struct PagerHandle {
    inner: Arc<Pager>,
}

#[derive(Debug)]
struct Pager {
    file: Arc<dyn VfsFile>,
    page_size: u32,
    cache: PageCache,
    header: Mutex<DatabaseHeader>,
    page_pool: Mutex<Vec<Vec<u8>>>,
    page_pool_max: usize,
    /// Cached main-database file page count (file length / page size).
    ///
    /// `load_page_from_disk` reads this instead of calling `file_size()`
    /// (`statx`) on every cache miss to decide whether a page is beyond EOF
    /// (and should yield a zeroed buffer instead of an error). It is updated
    /// on every write that extends the file and refreshed on
    /// `refresh_from_disk`, so it stays accurate for the single-process
    /// writer. Coordinated checkpoint refreshes must call `refresh_from_disk`
    /// before exposing another handle or process's new main-file length;
    /// uncoordinated external writers are unsupported.
    db_page_count: AtomicU32,
    #[cfg(test)]
    page_pool_reuse_count: std::sync::atomic::AtomicUsize,
}

impl PagerHandle {
    #[cfg(any(test, feature = "bench-internals"))]
    const DEFAULT_PAGE_POOL_MAX: usize = 256;

    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn open(
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
        cache_size_mb: usize,
    ) -> Result<Self> {
        Self::open_with_page_pool(file, header, cache_size_mb, Self::DEFAULT_PAGE_POOL_MAX)
    }

    pub(crate) fn open_with_page_pool(
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
        cache_size_mb: usize,
        page_pool_max: usize,
    ) -> Result<Self> {
        let page_size = header.page_size;
        // Seed the cached page count from the on-disk length once at open so
        // the read path doesn't pay a `statx` per cache miss just to learn
        // whether a page is beyond EOF.
        let db_page_count = page::page_count_for_len(file.file_size()?, page_size);
        Ok(Self::open_with_known_page_count(
            file,
            header,
            cache_size_mb,
            page_pool_max,
            db_page_count,
        ))
    }

    /// Opens the pager for a bootstrap written by
    /// `write_database_bootstrap_vfs`, whose main file is known to contain
    /// exactly the header page and reserved catalog-root page. This avoids a
    /// main-file stat while the fresh bootstrap durability barrier is running
    /// on another thread.
    pub(crate) fn open_fresh_bootstrap_with_page_pool(
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
        cache_size_mb: usize,
        page_pool_max: usize,
    ) -> Self {
        const FRESH_BOOTSTRAP_PAGE_COUNT: PageId = 2;
        Self::open_with_known_page_count(
            file,
            header,
            cache_size_mb,
            page_pool_max,
            FRESH_BOOTSTRAP_PAGE_COUNT,
        )
    }

    fn open_with_known_page_count(
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
        cache_size_mb: usize,
        page_pool_max: usize,
        db_page_count: PageId,
    ) -> Self {
        let page_size = header.page_size;
        let bytes = cache_size_mb.saturating_mul(1024 * 1024);
        let capacity_pages = (bytes / page_size as usize).max(1);
        Self {
            inner: Arc::new(Pager {
                file,
                page_size,
                cache: PageCache::new(capacity_pages, page_size as usize),
                header: Mutex::new(header),
                page_pool: Mutex::new(Vec::with_capacity(page_pool_max.min(256))),
                page_pool_max,
                db_page_count: AtomicU32::new(db_page_count),
                #[cfg(test)]
                page_pool_reuse_count: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        let handle = self
            .inner
            .cache
            .pin_or_load(page_id, || self.inner.load_page_from_disk(page_id))?;
        handle.read()
    }

    pub(crate) fn read_page_from_disk(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        Ok(Arc::from(self.inner.load_page_from_disk(page_id)?))
    }

    pub(crate) fn advise_sequential(&self) -> Result<()> {
        self.inner.file.advise_sequential()
    }

    /// Writes a single page directly to the database file and refreshes the
    /// page cache. Used by WAL tests and the bench-internals WAL fixture.
    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn write_page_direct(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        page::validate_page_id(page_id)?;
        if data.len() != self.inner.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match page size {}",
                data.len(),
                self.inner.page_size
            )));
        }

        write_all_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            data,
        )?;
        let required_len =
            page::page_offset(page_id, self.inner.page_size) + u64::from(self.inner.page_size);
        if self.inner.file.file_size()? < required_len {
            self.inner.file.set_len(required_len)?;
        }
        self.inner.bump_db_page_count_from_len(required_len);
        self.inner.cache.insert_clean_page(page_id, data.to_vec())
    }

    /// Writes a contiguous run of `pages.len()` pages starting at
    /// `start_page_id` in a single `pwrite` without refreshing the page cache.
    /// Used by checkpoint copyback to coalesce adjacent dirty pages (sorted by
    /// page id) into one syscall instead of one per page.
    ///
    /// The page cache is intentionally not populated: checkpoint clears the
    /// whole cache immediately after copyback via
    /// `invalidate_cache_after_local_checkpoint`, so
    /// inserting each copied page mid-copyback would only throw away one
    /// allocation + copy per page (~40k pages, ~160 MB of redundant memcpy on
    /// the full scale).
    ///
    /// `pages` must be exactly `page_size * count` bytes laid out as
    /// `start_page_id, start_page_id + 1, ...` so the byte range is one
    /// contiguous slice of the database file.
    pub(crate) fn write_pages_contiguous_no_cache(
        &self,
        start_page_id: PageId,
        pages: &[u8],
    ) -> Result<()> {
        page::validate_page_id(start_page_id)?;
        let page_size = self.inner.page_size as usize;
        if !pages.len().is_multiple_of(page_size) {
            return Err(DbError::internal(format!(
                "contiguous write buffer length {} is not a multiple of page size {page_size}",
                pages.len()
            )));
        }
        let count = pages.len() / page_size;
        if count == 0 {
            return Ok(());
        }
        let count = PageId::try_from(count)
            .map_err(|_| DbError::internal("contiguous database write contains too many pages"))?;
        start_page_id
            .checked_add(count.saturating_sub(1))
            .ok_or_else(|| DbError::internal("contiguous database write page range overflows"))?;
        let offset = page::page_offset(start_page_id, self.inner.page_size);
        let required_len = offset
            .checked_add(pages.len() as u64)
            .ok_or_else(|| DbError::internal("contiguous database write length overflows"))?;
        write_all_at(self.inner.file.as_ref(), offset, pages)?;
        // pwrite beyond EOF extends the file on every supported platform. A
        // checkpoint holds the cross-process writer/checkpoint gate, so no
        // supported concurrent truncation can race this copyback.
        self.inner.bump_db_page_count_from_len(required_len);
        Ok(())
    }

    /// Syncs the database file data and size to stable storage.
    ///
    /// Checkpoint copyback must call this after all main-file writes and
    /// before the WAL is truncated: once the WAL is discarded, the main file
    /// is the only remaining copy of the committed pages. See ADR 0004.
    ///
    /// The built-in VFS implementations guarantee that `sync_data` makes both
    /// file contents and the file length durable. On Linux this maps to
    /// `fdatasync`, which includes size changes needed to retrieve the data;
    /// macOS and Windows use the same durable flush as `sync_metadata`, and
    /// OPFS uses its synchronous access-handle flush. Timestamps and other
    /// recovery-irrelevant inode metadata need not be persisted on platforms
    /// that can omit them.
    pub(crate) fn sync_data(&self) -> Result<()> {
        self.inner.file.sync_data()
    }

    /// Reads the current file length exactly.
    ///
    /// Keep this for diagnostics, artifact copies, and freelist-tail
    /// truncation, where an out-of-band coordinated resize must be observed
    /// rather than inferred from this handle's last refresh.
    pub(crate) fn on_disk_page_count(&self) -> Result<PageId> {
        self.inner
            .file
            .file_size()
            .map(|size| page::page_count_for_len(size, self.inner.page_size))
    }

    /// Returns the main-file page count last established by pager open,
    /// pager-owned growth/shrink, or an explicit cross-process refresh.
    #[must_use]
    pub(crate) fn cached_page_count(&self) -> PageId {
        self.inner.db_page_count.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn allocate_page(&self) -> Result<PageId> {
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        if header.freelist.head_page_id != 0 {
            let page_id = header.freelist.head_page_id;
            let next = self.read_freelist_next(page_id)?;
            header.freelist.head_page_id = next;
            header.freelist.page_count = header.freelist.page_count.saturating_sub(1);
            self.persist_header(&header)?;
            self.inner.cache.discard(page_id)?;
            return Ok(page_id);
        }

        let page_id = self.on_disk_page_count()? + 1;
        let empty = self.inner.take_page_buffer()?;
        write_all_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            &empty,
        )?;
        self.inner.recycle_page_buffer(empty)?;
        let new_len =
            page::page_offset(page_id, self.inner.page_size) + u64::from(self.inner.page_size);
        self.inner.file.set_len(new_len)?;
        self.inner.bump_db_page_count_from_len(new_len);
        Ok(page_id)
    }

    #[cfg(test)]
    pub(crate) fn free_page(&self, page_id: PageId) -> Result<()> {
        if page_id <= page::CATALOG_ROOT_PAGE_ID {
            return Err(DbError::transaction(format!(
                "page {page_id} is reserved and cannot be freed"
            )));
        }
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        let page_bytes = encode_freelist_page(self.inner.page_size, header.freelist.head_page_id);
        write_all_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            &page_bytes,
        )?;
        header.freelist.head_page_id = page_id;
        header.freelist.page_count += 1;
        self.persist_header(&header)?;
        self.inner.cache.discard(page_id)
    }

    pub(crate) fn set_last_checkpoint_lsn(&self, lsn: u64) -> Result<()> {
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        header.last_checkpoint_lsn = lsn;
        self.persist_header(&header)
    }

    pub(crate) fn set_schema_cookie(&self, schema_cookie: u32) -> Result<()> {
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        header.schema_cookie = schema_cookie;
        self.persist_header(&header)
    }

    pub(crate) fn header_snapshot(&self) -> Result<DatabaseHeader> {
        self.inner
            .header
            .lock()
            .map(|header| header.clone())
            .map_err(|_| DbError::internal("pager header lock poisoned"))
    }

    pub(crate) fn header_from_disk(&self) -> Result<DatabaseHeader> {
        let mut bytes = [0_u8; DB_HEADER_SIZE];
        read_exact_at(self.inner.file.as_ref(), 0, &mut bytes)?;
        DatabaseHeader::decode(&bytes)
    }

    pub(crate) fn refresh_from_disk(&self, header: DatabaseHeader) -> Result<()> {
        if header.page_size != self.inner.page_size {
            return Err(DbError::corruption(format!(
                "database page size changed from {} to {}",
                self.inner.page_size, header.page_size
            )));
        }
        self.inner.cache.clear()?;
        // The on-disk length may have changed (e.g. checkpoint copyback grew
        // the file, or an external writer extended it); re-sync the cached
        // page count so the read path's EOF check is accurate.
        self.inner.refresh_db_page_count()?;
        *self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))? = header;
        Ok(())
    }

    /// Invalidates pages after this handle copied WAL versions into its own
    /// main database file.
    ///
    /// Unlike `refresh_from_disk`, this local checkpoint path does not need to
    /// stat the file: the checkpoint gate excludes an external writer and every
    /// positional copyback write already advanced `db_page_count`. The header
    /// page is WAL-managed, so copyback may have landed a newer committed
    /// header; `refresh_header_from_disk_after_local_checkpoint` reloads it
    /// before the freelist tail is truncated.
    pub(crate) fn invalidate_cache_after_local_checkpoint(&self) -> Result<()> {
        self.inner.cache.clear()
    }

    /// Reloads the database header from disk after this handle's checkpoint
    /// copyback.
    ///
    /// The header page is WAL-managed: commits that change the freelist stage
    /// page 0 through the WAL, so the in-memory header can be stale once
    /// copyback lands the newest committed header on the main file.
    /// `truncate_freelist_tail` must observe that committed freelist.
    ///
    /// Unlike `refresh_from_disk`, this does not clear the page cache again
    /// (copyback already invalidated it) and does not re-stat the file: every
    /// positional copyback write already advanced `db_page_count`, and the
    /// checkpoint gate excludes a concurrent resize.
    pub(crate) fn refresh_header_from_disk_after_local_checkpoint(&self) -> Result<()> {
        let header = self.header_from_disk()?;
        if header.page_size != self.inner.page_size {
            return Err(DbError::corruption(format!(
                "database page size changed from {} to {}",
                self.inner.page_size, header.page_size
            )));
        }
        *self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))? = header;
        Ok(())
    }

    pub(crate) fn truncate_freelist_tail(&self) -> Result<Option<PageId>> {
        let mut header = self.header_snapshot()?;
        if header.freelist.head_page_id == 0 || header.freelist.page_count == 0 {
            return Ok(None);
        }

        let current_page_count = self.on_disk_page_count()?;
        if current_page_count <= page::CATALOG_ROOT_PAGE_ID {
            return Ok(None);
        }

        let mut ordered_freelist_pages = Vec::with_capacity(header.freelist.page_count as usize);
        let mut page_id = header.freelist.head_page_id;
        while page_id != 0 {
            ordered_freelist_pages.push(page_id);
            page_id = self.read_freelist_next(page_id)?;
        }

        let freelist_pages = ordered_freelist_pages
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut new_page_count = current_page_count;
        let mut trimmed_pages = HashSet::new();
        while new_page_count > page::CATALOG_ROOT_PAGE_ID
            && freelist_pages.contains(&new_page_count)
        {
            trimmed_pages.insert(new_page_count);
            new_page_count = new_page_count.saturating_sub(1);
        }
        if trimmed_pages.is_empty() {
            return Ok(None);
        }

        let remaining_pages = ordered_freelist_pages
            .into_iter()
            .filter(|page_id| !trimmed_pages.contains(page_id))
            .collect::<Vec<_>>();
        for (index, page_id) in remaining_pages.iter().enumerate() {
            let next_page_id = remaining_pages.get(index + 1).copied().unwrap_or(0);
            write_all_at(
                self.inner.file.as_ref(),
                page::page_offset(*page_id, self.inner.page_size),
                &encode_freelist_page(self.inner.page_size, next_page_id),
            )?;
        }

        header.freelist.head_page_id = remaining_pages.first().copied().unwrap_or(0);
        header.freelist.page_count = header
            .freelist
            .page_count
            .saturating_sub(trimmed_pages.len() as u32);
        self.persist_header(&header)?;
        let new_len = page::page_offset(new_page_count.saturating_add(1), self.inner.page_size);
        self.inner.file.set_len(new_len)?;
        self.inner.set_db_page_count_from_len(new_len);
        self.inner.cache.clear()?;
        *self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))? = header;
        Ok(Some(new_page_count))
    }

    #[must_use]
    pub(crate) fn page_size(&self) -> u32 {
        self.inner.page_size
    }

    fn read_freelist_next(&self, page_id: PageId) -> Result<PageId> {
        let mut page = self.inner.take_page_buffer()?;
        read_exact_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            &mut page,
        )?;
        let next = decode_freelist_next(&page);
        self.inner.recycle_page_buffer(page)?;
        next
    }

    fn persist_header(&self, header: &DatabaseHeader) -> Result<()> {
        let bytes = header.encode();
        write_all_at(self.inner.file.as_ref(), 0, &bytes)?;
        self.inner
            .cache
            .insert_clean_page(page::HEADER_PAGE_ID, self.header_page(header))
    }

    fn header_page(&self, header: &DatabaseHeader) -> Vec<u8> {
        let mut page = page::zeroed_page(self.inner.page_size);
        page[..DB_HEADER_SIZE].copy_from_slice(&header.encode());
        page
    }
}

impl Pager {
    fn load_page_from_disk(&self, page_id: PageId) -> Result<Vec<u8>> {
        // Use the cached page count instead of a `file_size()` (`statx`) per
        // cache miss. The cached count is refreshed on every file-extending
        // write and on `refresh_from_disk`, so it tracks the on-disk length for
        // the owning writer.
        let page_count = self.db_page_count.load(Ordering::Acquire);
        if page_id > page_count {
            return self.take_page_buffer();
        }

        let mut data = self.take_page_buffer()?;
        read_exact_at(
            self.file.as_ref(),
            page::page_offset(page_id, self.page_size),
            &mut data,
        )?;
        Ok(data)
    }

    /// Bumps the cached main-db page count to cover `file_len` bytes if the
    /// file grew. Called after writes that may extend the file so the read
    /// path's EOF check stays accurate without a `statx` per cache miss.
    fn bump_db_page_count_from_len(&self, file_len: u64) {
        let new_count = page::page_count_for_len(file_len, self.page_size);
        self.db_page_count.fetch_max(new_count, Ordering::AcqRel);
    }

    /// Replaces the cached page count after a successful exact resize.
    fn set_db_page_count_from_len(&self, file_len: u64) {
        let new_count = page::page_count_for_len(file_len, self.page_size);
        self.db_page_count.store(new_count, Ordering::Release);
    }

    /// Re-reads the on-disk length and caches its page count. Used after
    /// operations that may have changed the file size out of band of the
    /// write paths above (e.g. `refresh_from_disk` after checkpoint, or
    /// `truncate_freelist_tail` shrinking the file).
    fn refresh_db_page_count(&self) -> Result<()> {
        let len = self.file.file_size()?;
        let count = page::page_count_for_len(len, self.page_size);
        self.db_page_count.store(count, Ordering::Release);
        Ok(())
    }

    fn take_page_buffer(&self) -> Result<Vec<u8>> {
        let page_size = self.page_size as usize;
        if self.page_pool_max != 0 {
            let mut pool = self
                .page_pool
                .lock()
                .map_err(|_| DbError::internal("pager page-pool lock poisoned"))?;
            if let Some(mut buffer) = pool.pop() {
                debug_assert!(buffer.capacity() >= page_size);
                buffer.resize(page_size, 0);
                buffer.fill(0);
                #[cfg(test)]
                self.page_pool_reuse_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(buffer);
            }
        }
        Ok(page::zeroed_page(self.page_size))
    }

    fn recycle_page_buffer(&self, mut buffer: Vec<u8>) -> Result<()> {
        let page_size = self.page_size as usize;
        if self.page_pool_max == 0 || buffer.capacity() < page_size {
            return Ok(());
        }
        buffer.clear();
        let mut pool = self
            .page_pool
            .lock()
            .map_err(|_| DbError::internal("pager page-pool lock poisoned"))?;
        if pool.len() < self.page_pool_max {
            pool.push(buffer);
        }
        Ok(())
    }

    #[cfg(test)]
    fn page_pool_available(&self) -> Result<usize> {
        self.page_pool
            .lock()
            .map(|pool| pool.len())
            .map_err(|_| DbError::internal("pager page-pool lock poisoned"))
    }

    #[cfg(test)]
    fn page_pool_reuse_count(&self) -> usize {
        self.page_pool_reuse_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader};
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs, VfsFile};

    use super::PagerHandle;
    use crate::storage::page;

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn repeated_reads_hit_vfs_once_when_page_is_cached() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("cache-hit");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        let payload = vec![0x7A; page::DEFAULT_PAGE_SIZE as usize];
        write_all_at(
            inner.as_ref(),
            page::page_offset(3, page::DEFAULT_PAGE_SIZE),
            &payload,
        )
        .expect("write page 3");
        inner
            .set_len(page::page_offset(4, page::DEFAULT_PAGE_SIZE))
            .expect("extend file");

        let counter = Arc::new(AtomicUsize::new(0));
        let file_size_counter = Arc::new(AtomicUsize::new(0));
        let file = Arc::new(CountingFile {
            inner,
            read_count: Arc::clone(&counter),
            file_size_count: Arc::clone(&file_size_counter),
            fail_file_size: false,
        });

        let pager = PagerHandle::open(file, header, 1).expect("open pager");
        let first = pager.read_page(3).expect("first read");
        let second = pager.read_page(3).expect("second read");

        assert_eq!(first.to_vec(), payload);
        assert_eq!(second.to_vec(), payload);
        assert_eq!(pager.cached_page_count(), 3);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(
            file_size_counter.load(Ordering::Relaxed),
            1,
            "pager open should stat once; page reads use the cached count"
        );
    }

    #[test]
    fn read_page_from_disk_bypasses_stale_cache() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("disk-bypass");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        let original = vec![0x11; page::DEFAULT_PAGE_SIZE as usize];
        write_all_at(
            inner.as_ref(),
            page::page_offset(3, page::DEFAULT_PAGE_SIZE),
            &original,
        )
        .expect("write original page");
        inner
            .set_len(page::page_offset(4, page::DEFAULT_PAGE_SIZE))
            .expect("extend file");

        let pager = PagerHandle::open(Arc::clone(&inner), header, 1).expect("open pager");
        assert_eq!(pager.read_page(3).expect("cached read").to_vec(), original);

        let updated = vec![0x22; page::DEFAULT_PAGE_SIZE as usize];
        write_all_at(
            inner.as_ref(),
            page::page_offset(3, page::DEFAULT_PAGE_SIZE),
            &updated,
        )
        .expect("overwrite page");

        assert_eq!(
            pager
                .read_page(3)
                .expect("cached page remains stale")
                .to_vec(),
            original
        );
        assert_eq!(
            pager
                .read_page_from_disk(3)
                .expect("disk bypass read")
                .to_vec(),
            updated
        );
    }

    #[test]
    fn file_growth_and_freelist_reuse_are_deterministic() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("freelist");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header, 1).expect("open pager");

        let first_allocated = pager.allocate_page().expect("allocate page");
        assert_eq!(first_allocated, 3);
        assert_eq!(pager.on_disk_page_count().expect("page count"), 3);

        pager.free_page(first_allocated).expect("free page");
        let reused = pager.allocate_page().expect("reuse freelist page");
        assert_eq!(reused, 3);
    }

    #[test]
    fn write_page_direct_does_not_shrink_existing_file() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("write-page-direct-no-shrink");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        file.set_len(page::page_offset(5, page::DEFAULT_PAGE_SIZE))
            .expect("extend file to four pages");

        let pager = PagerHandle::open(file, header, 4).expect("open pager");
        let page_four = vec![0x4A; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(4, &page_four)
            .expect("seed page four");
        let original_len = pager
            .inner
            .file
            .file_size()
            .expect("file size after seeding page four");

        let page_two = vec![0x2B; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(2, &page_two)
            .expect("rewrite smaller page id");

        assert_eq!(
            pager
                .inner
                .file
                .file_size()
                .expect("file size after rewrite"),
            original_len
        );
        assert_eq!(
            pager.read_page(4).expect("read page four").to_vec(),
            page_four
        );
    }

    #[derive(Debug)]
    struct CountingFile {
        inner: Arc<dyn VfsFile>,
        read_count: Arc<AtomicUsize>,
        file_size_count: Arc<AtomicUsize>,
        fail_file_size: bool,
    }

    impl VfsFile for CountingFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
            self.read_count.fetch_add(1, Ordering::Relaxed);
            self.inner.read_at(offset, buf)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
            self.inner.write_at(offset, buf)
        }

        fn advise_sequential(&self) -> crate::Result<()> {
            self.inner.advise_sequential()
        }

        fn sync_data(&self) -> crate::Result<()> {
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> crate::Result<()> {
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> crate::Result<u64> {
            self.file_size_count.fetch_add(1, Ordering::Relaxed);
            if self.fail_file_size {
                return Err(crate::DbError::internal("injected file-size failure"));
            }
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> crate::Result<()> {
            self.inner.set_len(len)
        }
    }

    #[test]
    fn pager_open_propagates_file_size_errors() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("open-file-size-error");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        let file = Arc::new(CountingFile {
            inner,
            read_count: Arc::new(AtomicUsize::new(0)),
            file_size_count: Arc::new(AtomicUsize::new(0)),
            fail_file_size: true,
        });

        let error = PagerHandle::open(file, header, 1).expect_err("open should fail");
        assert!(error.to_string().contains("injected file-size failure"));
    }

    #[test]
    fn contiguous_write_updates_cached_page_count_and_checks_page_range() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("contiguous-write-page-count");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        let file_size_counter = Arc::new(AtomicUsize::new(0));
        let file = Arc::new(CountingFile {
            inner,
            read_count: Arc::new(AtomicUsize::new(0)),
            file_size_count: Arc::clone(&file_size_counter),
            fail_file_size: false,
        });
        let pager = PagerHandle::open(file, header, 1).expect("open pager");
        let mut pages = vec![0x33; page::DEFAULT_PAGE_SIZE as usize];
        pages.extend(vec![0x44; page::DEFAULT_PAGE_SIZE as usize]);

        pager
            .write_pages_contiguous_no_cache(3, &pages)
            .expect("write pages 3 and 4");
        let page_four = pager
            .read_page_from_disk(4)
            .expect("cached count includes page 4");
        assert!(page_four.iter().all(|byte| *byte == 0x44));
        assert_eq!(pager.cached_page_count(), 4);
        assert_eq!(
            file_size_counter.load(Ordering::Relaxed),
            1,
            "only open stats the file; the authoritative contiguous write and read do not"
        );

        let overflow = pager.write_pages_contiguous_no_cache(u32::MAX, &pages);
        assert!(overflow.is_err(), "page-id range overflow must be rejected");
    }

    #[test]
    fn refresh_from_disk_restats_and_replaces_cached_page_count_after_external_shrink() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("refresh-page-count-shrink");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        inner
            .set_len(page::page_offset(6, page::DEFAULT_PAGE_SIZE))
            .expect("extend database to five pages");
        let file_size_counter = Arc::new(AtomicUsize::new(0));
        let file = Arc::new(CountingFile {
            inner: Arc::clone(&inner),
            read_count: Arc::new(AtomicUsize::new(0)),
            file_size_count: Arc::clone(&file_size_counter),
            fail_file_size: false,
        });
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");
        assert_eq!(pager.cached_page_count(), 5);
        assert_eq!(file_size_counter.load(Ordering::Relaxed), 1);

        inner
            .set_len(page::page_offset(4, page::DEFAULT_PAGE_SIZE))
            .expect("externally shrink database to three pages");
        pager
            .refresh_from_disk(header)
            .expect("refresh externally changed database length");
        assert_eq!(pager.cached_page_count(), 3);
        assert_eq!(
            file_size_counter.load(Ordering::Relaxed),
            2,
            "explicit refresh must retain one exact stat for out-of-band changes"
        );
    }

    fn unique_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic wall clock")
            .as_nanos();
        let ordinal = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        PathBuf::from(format!(
            ":memory:{label}:{}:{stamp}:{ordinal}",
            std::process::id()
        ))
    }

    #[test]
    fn header_from_disk_reads_header_written_to_vfs() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("header-from-disk");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");
        let on_disk = pager.header_from_disk().expect("read header from disk");
        assert_eq!(on_disk, header);
    }

    #[test]
    fn set_last_checkpoint_and_schema_cookie_persist_to_disk() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("persist-header");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        let original_database_id = header.database_id;
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");

        pager.set_last_checkpoint_lsn(0xDEADBEEF).expect("set lsn");
        pager.set_schema_cookie(0xBEEF).expect("set schema cookie");

        let on_disk = pager.header_from_disk().expect("read header from disk");
        assert_eq!(on_disk.database_id, original_database_id);
        assert_eq!(on_disk.last_checkpoint_lsn, 0xDEADBEEF);
        assert_eq!(on_disk.schema_cookie, 0xBEEF);
    }

    #[test]
    fn refresh_from_disk_detects_page_size_change() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("refresh-page-size");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");

        let bad_header = DatabaseHeader::new(header.page_size * 2);
        let res = pager.refresh_from_disk(bad_header);
        assert!(res.is_err());
    }

    #[test]
    fn free_reserved_page_returns_error() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("free-reserved");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header, 1).expect("open pager");

        let res = pager.free_page(page::CATALOG_ROOT_PAGE_ID);
        assert!(res.is_err());
    }

    #[test]
    fn read_page_beyond_count_returns_zeroed_page() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("read-beyond");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");

        let data = pager.read_page(100).expect("read beyond");
        assert_eq!(data.to_vec(), page::zeroed_page(page::DEFAULT_PAGE_SIZE));
    }

    #[test]
    fn page_pool_recycles_buffers() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("page-pool");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create db");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager =
            PagerHandle::open_with_page_pool(file, header, 1, 10).expect("open pager with pool");

        let mut buffers = Vec::new();
        for _ in 0..10 {
            buffers.push(pager.inner.take_page_buffer().expect("take fresh buffer"));
        }
        assert_eq!(pager.inner.page_pool_available().expect("pool size"), 0);
        assert_eq!(pager.inner.page_pool_reuse_count(), 0);

        for buffer in buffers.drain(..) {
            pager
                .inner
                .recycle_page_buffer(buffer)
                .expect("recycle buffer");
        }
        assert_eq!(pager.inner.page_pool_available().expect("pool size"), 10);

        for _ in 0..10 {
            buffers.push(pager.inner.take_page_buffer().expect("reuse buffer"));
        }
        assert_eq!(pager.inner.page_pool_reuse_count(), 10);
        assert!(buffers
            .iter()
            .all(|buffer| buffer.len() == page::DEFAULT_PAGE_SIZE as usize));
        assert!(buffers
            .iter()
            .all(|buffer| buffer.iter().all(|byte| *byte == 0)));
    }
}
