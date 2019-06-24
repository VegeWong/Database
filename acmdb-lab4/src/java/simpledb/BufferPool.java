package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int _maxPageNum;
    private ConcurrentHashMap<PageId, Page> _pidMappedPage;
    private ConcurrentHashMap<Integer, LinkedList<PageId>> _cachedTable;
    private LockManager lockManager;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        _maxPageNum = numPages;
        _pidMappedPage = new ConcurrentHashMap<PageId, Page>();
        _cachedTable = new ConcurrentHashMap<Integer, LinkedList<PageId>>();
        lockManager = new LockManager();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        // Check Lock
//        LockManager.Locker locker = lockManager.getLocker(tid);
//        if (!locker.lock(pid, perm))
//            throw new TransactionAbortedException();

        Page _page = _pidMappedPage.get(pid);
        if (_page == null) try {
            if (_pidMappedPage.size() == _maxPageNum)
                evictPage();
            DbFile _file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            _page = _file.readPage(pid);
            _pidMappedPage.put(pid, _page);
            LinkedList<PageId> pids = _cachedTable.get(pid.getTableId());
            if (pids == null) {
                pids = new LinkedList<PageId>();
                _cachedTable.put(pid.getTableId(), pids);
            }
            pids.add(pid);
        } catch (NoSuchElementException e) {
            System.out.println(e.toString());
            throw new DbException("Get Dbfile failed");
        }

        return _page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.getLocker(tid).release(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.getLocker(tid).clear();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.getLocker(tid).holdsLock(p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f =  Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPgList = f.insertTuple(tid, t);

        trimCache(tid, dirtyPgList);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPgList = f.deleteTuple(tid, t);

        trimCache(tid, dirtyPgList);
    }

    private void trimCache(TransactionId tid, ArrayList<Page> dirtyPages)
        throws DbException {
        Page curPg;
        Iterator<Page> pgItr = dirtyPages.iterator();
        while (pgItr.hasNext()) {
            if (_pidMappedPage.size() == _maxPageNum)
                evictPage();
            curPg = pgItr.next();
            _pidMappedPage.put(curPg.getId(), curPg);
//            int echoSize = _pidMappedPage.size();
            curPg.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> e : _pidMappedPage.entrySet()) {
            Page pg = e.getValue();
            if (pg.isDirty() == null)
                continue;
            DbFile f = Database.getCatalog().getDatabaseFile(pg.getId().getTableId());
            f.writePage(pg);
            pg.markDirty(false, null);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        _pidMappedPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page pg = _pidMappedPage.get(pid);

        if (pg == null || pg.isDirty() == null)
            return;
        DbFile f = Database.getCatalog().getDatabaseFile(pg.getId().getTableId());
        f.writePage(pg);
        pg.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        Random rand = new Random();
        PageId[] cachedKeys = _pidMappedPage.keySet().toArray(new PageId[0]);
        PageId pid = cachedKeys[rand.nextInt(cachedKeys.length)];

        try {
            flushPage(pid);
            _pidMappedPage.remove(pid);
        } catch (IOException e) {
            throw new DbException("Writing Failed: Can not write file on disk");
        }
    }

    public LinkedList<PageId> getCachedPagesOfTable(int tableId) {
//        return _cachedTable.get(tableId);
        return null;
    }
}