package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TxLockManager;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    public File f;
    private TupleDesc td;

    // 因为一表一个HeapFile, 所以放心使用一个tableId表示一个Heapfile
    private int tableId;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if(this.tableId != pid.getTableId()) return null;
        // 先计算偏差
        int offset = BufferPool.getPageSize() * pid.getPageNumber();
        RandomAccessFile randomAccessFile = null;
        HeapPage heapPage = null;
        try {
            randomAccessFile = new RandomAccessFile(f,"r");
            randomAccessFile.seek(offset);
            byte[] buffer = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(buffer,0,BufferPool.getPageSize());
            randomAccessFile.close();
            HeapPageId heapPageId = (HeapPageId) pid;
            heapPage = new HeapPage(heapPageId, buffer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return heapPage;
    }

    // 将指定的page写到磁盘上
    public void writePage(Page page) throws IOException {
        HeapPage heapPage = (HeapPage) page;
        int offset = heapPage.getId().getPageNumber() * BufferPool.getPageSize();
        try {
            RandomAccessFile rw = new RandomAccessFile(f, "rw");
            rw.seek(offset); // 记得seek
            rw.write(heapPage.getPageData());
            rw.close();
        }catch (IOException e){
            throw e;
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // 这个公式是没有错的;
        return  (int) Math.floor(f.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        boolean insertSuccess = false; // 需要有一个flag来标志我们是否插入成功
        List<Page> res = new ArrayList<>();
        for (int i =0; i < numPages(); i ++) {
            HeapPageId pageId = new HeapPageId(tableId, i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            int numUnusedSlots = page.getNumUnusedSlots();
            if(numUnusedSlots != 0){
                page.insertTuple(t);
                // page.markDirty(true,tid); 这边暂时不要markDirty，交给buffer-pool
                insertSuccess = true;
                res.add(page); // 只返回这个修改的page
                break;
            } else {
                // 现在这里可以释放锁了, 因为这个页面我们没有使用
                try {

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        // !!!如果没有页可以容纳，那么我们就添加一个新页
        if(!insertSuccess){
            synchronized (this){
                // 开始创建一个新的page,准备写到磁盘上;
                byte[] empty = HeapPage.createEmptyPageData();
                HeapPage heapPage = new HeapPage(new HeapPageId(tableId, numPages()), empty);
                heapPage.insertTuple(t);
                try {
                    Database.getBufferPool().getPage(tid, heapPage.pid, Permissions.READ_WRITE);
                }catch (Exception e){
                    e.printStackTrace();
                }
                writePage(heapPage);
                res.add(heapPage); // 记得加到res中;
            }
        }
        return res;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        List<Page> res = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        // page.markDirty(true,tid); 这边暂时不要markDirty，交给buffer-pool
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    // 自建类，用于迭代每一个page，通过page进而迭代每一个tuple；
    private class HeapFileIterator implements DbFileIterator{


        private int pageCursor; // 标记我们遍历到哪一个page了;

        private Iterator<Tuple> inPageCursor; // cursor "within" a page，在页内做索引

        private TransactionId tid;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, 0), Permissions.READ_ONLY);
            inPageCursor = page.iterator();
        }

        private HeapPage prefetchPage() throws TransactionAbortedException, DbException {
            if(pageCursor == numPages() - 1) return null;
            return (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId,pageCursor + 1), Permissions.READ_ONLY);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(pageCursor == numPages() || inPageCursor == null ) return false;
            if(inPageCursor.hasNext()) return true; // 如果正确的话，那直接返回；
            // 到了这里说明：当前页不是最后一页且当前页已经没有tuple了，不断检查下一页：
            while (true){
                HeapPage nxtPage = prefetchPage();
                if(nxtPage == null) return false;
                pageCursor ++; // 更新pageCursor，防止next()调用出错；
                inPageCursor = nxtPage.iterator(); // 这边要及时更新两个Cursor,不然next()会出错
                if(inPageCursor.hasNext()) return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // 说明这个 page 已经遍历完了, 再取一个
            while(!inPageCursor.hasNext()){
                // 这边也要更新，不能依赖hasNext去更新，next自己也要有措施；
                HeapPage nextPage = prefetchPage();
                if(nextPage == null) throw  new NoSuchElementException();
                inPageCursor = nextPage.iterator();
                pageCursor ++;
            }
            return inPageCursor.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            inPageCursor = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, 0), Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void close() {
            inPageCursor = null;
        }
    }

}

