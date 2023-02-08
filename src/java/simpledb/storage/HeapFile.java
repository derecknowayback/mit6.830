package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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

    private File f;
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
        FileInputStream inputStream = null;
        HeapPage heapPage = null;
        try {
            inputStream = new FileInputStream(f);
            inputStream.skip(offset); // 跳过前面的page
            byte[] buffer = new byte[BufferPool.getPageSize()];
            inputStream.read(buffer,0,BufferPool.getPageSize());
            HeapPageId heapPageId = (HeapPageId) pid;
            heapPage = new HeapPage(heapPageId, buffer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor(f.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator();
    }

    // 自建类，用于迭代每一个page，通过page进而迭代每一个tuple；
    private class HeapFileIterator implements DbFileIterator{


        private int pageCursor; // 标记我们遍历到哪一个page了;

        private Iterator<Tuple> inPageCursor; // cursor "within" a page，在页内做索引

        private List<HeapPage> pageList; // 在 pageList


        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageList = new ArrayList<>();
            pageCursor = 0;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(tableId, 0), null);
            pageList.add(page);
            inPageCursor = page.iterator();
        }

        private HeapPage prefetchPage() throws TransactionAbortedException, DbException {
            HeapPage nxtPage;
            if(pageCursor == pageList.size() - 1 )
                nxtPage = (HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(tableId, pageList.size()), null);
            else
                nxtPage = pageList.get(pageCursor + 1);
            pageList.add(nxtPage);
            return nxtPage;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(pageCursor == numPages() || pageList == null || pageList.isEmpty()) return false;
            if(inPageCursor.hasNext()) return true; // 如果正确的话，那直接返回；
            if(pageCursor == numPages() - 1) return false; // 如果已经是最后一页，而且上一个if不正确，那直接返回false；
            // 到了这里说明：当前页不是最后一页且当前页已经没有tuple了，检查下一页：
            HeapPage nxtPage = prefetchPage();
            pageCursor ++; // 更新pageCursor，防止next()调用出错；
            inPageCursor = nxtPage.iterator(); // 这边要及时更新两个Cursor,不然next()会出错
            return inPageCursor.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()) throw new NoSuchElementException();
            // 说明这个 page 已经遍历完了, 再取一个
            if(!inPageCursor.hasNext()){
                // 这边也要更新，不能依赖hasNext去更新，next自己也要有措施；
                inPageCursor = prefetchPage().iterator();
                pageCursor ++;
            }
            return inPageCursor.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            inPageCursor = pageList.get(pageCursor).iterator();
        }

        @Override
        public void close() {
            pageList.clear();
        }
    }

}

