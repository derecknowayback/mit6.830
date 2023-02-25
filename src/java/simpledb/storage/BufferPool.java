package simpledb.storage;


import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TxLockManager;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 500;

    private final int numPages; //  表示当前缓存池的容量

    private ConcurrentHashMap<PageId,Page> pageMap; // 根据 PageId 和 Page 做映射
    private ConcurrentHashMap<PageId,Integer> lruMap; // 保存page的访问次数
    public TxLockManager txLockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageMap = new ConcurrentHashMap<>();
        this.lruMap = new ConcurrentHashMap<>();
        this.txLockManager = new TxLockManager();
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
            try {
                // 获取线程池
                // Future用于执行多线程的执行结果
                txLockManager.acquireLock(tid,pid,perm,System.currentTimeMillis(),0);
            }catch (Exception e){
                throw new TransactionAbortedException();
            }
        // permission 保证
        Page page = pageMap.get(pid);
        if(page != null)  {
            lruMap.merge(pid,1,Integer::sum);
            return page;
        }
        // 如果容量已满, 就要执行驱除
        if(pageMap.size() == numPages){
            evictPage();
        }
        boolean isLocked = txLockManager.acquireLock(tid, pid, perm, System.currentTimeMillis(), 0);
        if (!isLocked) throw new TransactionAbortedException();
        page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pageMap.put(page.getId(), page);
        lruMap.merge(pid,1,Integer::sum);
        return page;
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        try {
            txLockManager.releaseLock(tid, pid);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid,true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return txLockManager.hasHoldsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        String action = commit ? "提交 commit" : "终止 abort";
        System.out.println("事务"+tid.getId() +action);
        // 提交时，应该将与事务关联的脏页刷新到磁盘。
        // 中止时，应该通过将页面恢复到其磁盘上状态来恢复事务所做的任何更改。
        if(commit){
            try {
                flushPages(tid);
            }catch (IOException e){
                e.printStackTrace();
            }
        }else{
            try {
                recoverAllPages(tid);
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        try {
            txLockManager.releaseLock(tid);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void recoverAllPages(TransactionId tid) throws IOException{
        for (PageId pid: pageMap.keySet()) {
            Page page = pageMap.get(pid);
            if(!tid.equals(page.isDirty()))continue;
            Page recovery = Database.getCatalog().getDatabaseFile(page.getId().getTableId()).readPage(page.getId());
            pageMap.put(recovery.getId(), recovery);
            // lruMap不做改动;
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true,tid); // 标记为脏页
            PageId pageId = page.getId();
            pageMap.put(pageId,page); // 更新缓存池中的page
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 先拿到表id
        int tableId = t.getRecordId().getPageId().getTableId();
        // 再根据表id找到DBFile,通过Catalog
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid,t);
        for (Page page : pages) {
            page.markDirty(true,tid); // 标记为脏页
            PageId pageId = page.getId();
            pageMap.put(pageId,page); // 更新缓存池中的page
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId id : pageMap.keySet()) {
            Page page = pageMap.get(id);
            // 如果是脏页，写会磁盘
            if(page.isDirty() != null){
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(id.getTableId());
                databaseFile.writePage(page);
                try {
                    txLockManager.releaseLock(page.isDirty(), page.getId());
                } catch (Exception e){
                    e.printStackTrace();
                }
                page.markDirty(false,null);
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
       pageMap.remove(pid);
       lruMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // 这个应该不需要管buffer-pool有没有吧,但是保险起见我还是判断了 page != null
        Page page = pageMap.get(pid);
        if(page != null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false,null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (PageId pageId: pageMap.keySet()) {
            Page page = pageMap.get(pageId);
            if(!tid.equals(page.isDirty())) continue;
            flushPage(pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId victim = null;
        int min = Integer.MAX_VALUE;
        // 找到最少访问的page
        for (PageId id: pageMap.keySet()) {
            Page page = pageMap.get(id);
            if(page.isDirty() != null) continue;
            if(victim == null){
                victim = id;
                min = lruMap.get(id);
            }else{
                int times = lruMap.get(id);
                if (times < min){
                    victim = id;
                    min = times;
                }
            }
        }
        if(victim == null) throw new DbException("NO CLEAN PAGE TO EVICT");
        try {
            flushPage(victim);
            // 记得要驱除
            lruMap.remove(victim);
            pageMap.remove(victim);
        }catch (IOException e){
            throw new DbException("Evict error: failed to flush...");
        }
    }

}
