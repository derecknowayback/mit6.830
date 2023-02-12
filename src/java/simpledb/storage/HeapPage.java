package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header; // bitmap
    final Tuple[] tuples; // 真正存储tuple的地方
    final int numSlots; // 槽的容量
    final List<Integer> tupleList; // 有效tuple的index集合
    final List<Integer> unusedList; // 没有使用的Slot,作为缓存,高效获得unused slot

    private boolean isDirty; // 这个字段其实没什么用, 真正有用的是 transactionId
    private TransactionId transactionId;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.tupleList = new ArrayList<>();
        this.unusedList = new ArrayList<>();
        this.isDirty = false;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();
        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     * tuples per page = floor((page size * 8) / (tuple size * 8 + 1))
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        return (int) Math.floor((BufferPool.getPageSize() * 8.0 ) / (td.getSize() * 8 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * header bytes = ceiling(tuples per page / 8)
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int) Math.ceil(getNumTuples() / 8.0);
    }



    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * 用来创建一个新的empty-page,这个方法在对HeapFile追加一个新页的时候很有用
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        int index = t.getRecordId().getTupleNumber(); // 拿到页内位置
        if(t.getRecordId().getPageId() != pid || !isSlotUsed(index)) // 判断是否有这个tuple
            throw new DbException("Delete failed ...");
        markSlotUsed(index,false); // 标记为free
        tupleList.remove((Integer)index); // 从tupleList中移除
        unused.add(index); // 添加到unusedList
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // 检查是不是这个page的
        if(getNumUnusedSlots() == 0 || !td.equals(t.getTupleDesc()))
            throw new DbException("ERROR: HeapPage Insert failed ...");
        int index = unusedList.remove(0); // pop出第一个元素
        tuples[index] = t;
        markSlotUsed(index,true); // 标记为已使用
        tupleList.add(index);
        t.setRecordId(new RecordId(pid,index)); // !!!不要忘记设置RecordId
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.isDirty = dirty;
        // 不知道要不要判断, 防御性起见还是判断一下
        if(dirty)
            this.transactionId = tid;
        else
            this.transactionId = null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return transactionId;
    }

    /**
     * Returns the number of unused (i.e., empty) slots on this page.
     */
    public int getNumUnusedSlots() {
        int res = 0, index = -1;
        for (int i = 0; i < getNumTuples(); i++) {
            if(i % 8 == 0) index++;
            if(getBit(header[index], i % 8) == 0){
                res++;
                tupleList.remove((Integer) i); // 这里特地强转一下，不然会被识别为 "按索引移除"
                unusedList.add(i); // 这里加一个缓存
            }
            else if (!tupleList.contains(i)){
                tupleList.add(i);
                unusedList.remove((Integer) i); // 这里特地强转一下，不然会被识别为 "按索引移除"
            }
        }
        return res;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int index = i / 8;
        return getBit(header[index], i % 8) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int bit = value ? 1 : 0, index = i / 8;
        header[index] = setBit(header[index],i % 8,bit);
    }

    // 自建类，用于迭代页面上的所有tuple
    private class TupIterator implements Iterator{

        Iterator <Integer> realIterator; // 这个iterator实际上是tupleList的iterator，我们包装一下；

        public TupIterator(Iterator <Integer> realIterator){
            this.realIterator = realIterator;
        }

        @Override
        public boolean hasNext() {
            return realIterator.hasNext();
        }

        @Override
        public Tuple next() {
            return tuples[realIterator.next()];
        }

        @Override
        public void remove() throws UnsupportedOperationException{
            throw new UnsupportedOperationException();
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     *         (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        getNumUnusedSlots(); // 更新tupleList，确保正确性；
        return new TupIterator(tupleList.iterator());
    }

    /* Returns the Nth bit of X. */
    private int getBit(byte x, int n) {
        int mask = 1 << n;
        return (x & mask) >> n;
    }

    /* Set the nth bit of the value of x to v. */
    private byte setBit(byte x, int n, int v) {
        int bit = getBit(x,n);
        int mask = (bit ^ v) << n;
        return (byte) (mask ^ x);
    }
}

