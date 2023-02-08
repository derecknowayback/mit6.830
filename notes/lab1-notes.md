# SimpleDB 整体架构

lab1不需要做任何关于事务的事情。

SimpleDB包括:

- 表示字段、元组和schema的类；
- 谓词和条件的类；
- 一个或多个access method(例如，堆文件)，关系将存储在磁盘上，并提供一种方法来遍历这些关系的元组；
- 处理元组的操作符类的集合(例如，选择、连接、插入、删除等)；
- 一个缓冲池，在内存中缓存活动元组和页面，并处理并发控制和事务(在这个实验中，您不需要担心这两个);
- 存储可用表及其模式信息的目录 aka.catalog。

SimpleDB不包括：

- 一个允许您直接在SimpleDB中键入查询的SQL前端或解析器。相反，查询是通过将一组操作符链接到一个手工构建的查询计划中来构建的(请参阅第2.7节)。我们将提供一个简单的解析器供以后的实验使用。
- 视图。
- 除整数和固定长度字符串外的数据类型。



# Database

提供了对静态对象集合的访问，这些对象是数据库的全局状态。特别是，包括访问目录(数据库中所有表的列表)、缓冲池(当前驻留在内存中的数据库文件页的集合)和日志文件的方法。





# Fields & Tuples

SimpleDB中的元组是非常简单的：

- 它们由`Field`对象的集合组成，Tuple中的<u>每个字段一个</u>。
- 字段是不同数据类型(例如，整数，字符串)实现的接口。
- 元组对象由底层访问方法创建(例如，堆文件或b -树)。
- 元组有一个schema，称为元组描述符，由TupleDesc对象表示。TupleDesc对象由Type对象的集合组成，在元组中每个字段一个，每个字段描述对应字段的类型。



Exercise 1

实现下面2个类：

> - src/java/simpledb/storage/TupleDesc.java
> - src/java/simpledb/storage/Tuple.java

代码应该通过`TupleTest`和`TupleDescTest`单元测试。此时，`modifyRecordId()`应该会失败，因为您还没有实现它。



## Tuple.java

```java
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<Field> fields;  // fields: 字段的集合

    private TupleDesc tupleDesc; // tupleDesc：schema

    private RecordId recordId; // recordId: 记录的id号

    /**
     * Create a new tuple with the specified schema (type).
     */
    public Tuple(TupleDesc td) {
        this.fields = new ArrayList<>();
        if(td == null || td.numFields() < 1){
            return;
        }
        this.tupleDesc = td;
        int len = td.numFields();
        // 创建字段集合
        for (int i = 0; i < len; i++) {
            Field newField;
            if(td.getFieldType(i) == Type.INT_TYPE)
                newField = new IntField(0);
            else
                newField = new StringField("",0);
            fields.add(newField);
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder res = new StringBuilder();
        for (Field field:fields) {
            String value;
            if(field instanceof  IntField)
                value = String.valueOf(((IntField) field).getValue());
            else
                value = ((StringField) field).getValue();
            res.append(value).append('\t');
        }
        // 去除最后一个 \t
        return res.substring(0, res.length() - 1);
    }
}
```



## TupleDesc.java

```java
public class TupleDesc implements Serializable {
    
    /**
     * 这是一个内部类, 用来组织一个schema
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * 字段的类型
         */
        public final Type fieldType;

        /**
         * 字段名字
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TDItem)) return false;
            TDItem another = (TDItem) obj;
            boolean b1, b2;
            b1 = this.fieldType == another.fieldType;
            b2 = (this.fieldName == null && another.fieldName == null) || (this.fieldName != null && this.fieldName.equals(another.fieldName));
            return b1 && b2;
        }
    }

    /**
     * 添加一个TDItem集合
     */
    private List<TDItem> typeList;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int len;
        // check length
        if (typeAr == null || (len = typeAr.length) == 0) {
            System.out.println("LENGTH ERROR: Create TupleDesc Failed ...");
            return;
        }
        boolean hasName = fieldAr != null;
        // 创建 typeList
        this.typeList = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            if(hasName)
                typeList.add(new TDItem(typeAr[i], fieldAr[i]));
            else
                typeList.add(new TDItem(typeAr[i], null));
        }
    }


    public TupleDesc(Type[] typeAr) {
        this(typeAr,null); // 直接调用上一个构造函数;
    }

  
    /**
     * Find the index of the field with a given name.
     *
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        int len, i = 0;
        if (typeList == null || (len = typeList.size()) == 0) {
            throw new NoSuchElementException();
        }
        for (i = 0; i < len; i++) {
            // what if name == null ? 如果是null的话, 返回第一个 null
            if (name != null && name.equals(typeList.get(i).fieldName)
                    || (name == null && typeList.get(i).fieldName == null))
                break;
        }
        if (i == len) throw new NoSuchElementException();
        return i;
    }

    /**
     * 获取tuple的size, tuple被设计为定长;
     */
    public int getSize() {
        int len, res = 0;
        if (typeList == null || (len = typeList.size()) == 0) return 0;
        for (int i = 0; i < len; i++) {
            res += typeList.get(i).fieldType.getLen();
        }
        return res;
    }


    /**
     * 合并两个schema,得到一个新的schema
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td1 == null || td2 == null) return null;
        TupleDesc res = new TupleDesc();
        for (int i = 0; i < td1.numFields(); i++) {
            res.typeList.add(td1.getItem(i));
        }
        for (int i = 0; i < td2.numFields(); i++) {
            res.typeList.add(td2.getItem(i));
        }
        return res;
    }


    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        int len;
        if(typeList == null || (len = typeList.size()) == 0) return null;
        for (int i = 0; i < len; i++) {
            TDItem tdItem = typeList.get(i);
            builder.append(tdItem.fieldType).append("(").append(tdItem.fieldName).append(")");
        }
        return builder.toString();
    }
}
```





# Catalog 

目录(Catalog类)由当前在数据库中的表和表的schema列表组成。需要支持添加新表的功能，以及获取关于特定表的信息。与每个表相关联的是一个TupleDesc对象，该对象允许操作符确定表中字段的类型和数量。

全局catalog是为整个SimpleDB流程分配的单个catalog实例。全局catalog可以通过方法Database.getCatalog()获取实例。



Exercise 2

实现 ->

> src/java/simpledb/common/Catalog.java

代码应该通过CatalogTest中的单元测试。



## Catalog.java

```java
/**
 * 目前的catalog无法从磁盘上读取一个catalog表,后续会有；
 */
public class Catalog {

    /**
     *  自建的内部类, 表示一个目录的条目, 一个条目代表一个表
     */
    private static class CataLogEntry{
        DbFile file; // 具体的DbFile文件
        String name; // 条目的名字, 也就是表名
        String pkeyField; // 表的主键名称
        public CataLogEntry(DbFile file, String name, String pkeyField){
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }
    }

    private HashMap<Integer,CataLogEntry> cataLogEntries; // 哈希表, 用于字典

    private HashMap<String,Integer> nameIdMap; // 表名和表id的映射, 加速名字查找;

    private List<Integer> idList; // 表id 的集合, 方便遍历;
    
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        this.cataLogEntries = new HashMap<>();
        this.nameIdMap = new HashMap<>();
        this.idList = new ArrayList<>();
    }

    /**
     * 添加表
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        CataLogEntry entry = new CataLogEntry(file, name, pkeyField);
        int id = file.getId();
        cataLogEntries.put(id, entry);
        idList.add(id);
        nameIdMap.put(name,id);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }


    /**
     * 根据表名返回表id
     */
    public int getTableId(String name) throws NoSuchElementException {
        Integer id = nameIdMap.get(name);
        if(id == null){
            throw new NoSuchElementException();
        }
        return id;
    }

    /**
     * 根据表id返回schema
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        CataLogEntry entry = cataLogEntries.get(tableid);
        if(entry == null){
            throw new NoSuchElementException();
        }
        return entry.file.getTupleDesc();
    }


    public Iterator<Integer> tableIdIterator() {
        return idList.iterator();
    }


    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        cataLogEntries.clear();
        idList.clear();
        nameIdMap.clear();
    }
}
```







# Buffer pool

缓冲池(SimpleDB中的类BufferPool)负责在内存中缓存最近从磁盘读取的页面。所有操作符通过缓冲池从磁盘上的各种文件读取和写入页面。它由固定数量的页组成，由BufferPool构造函数的numPages参数定义。在后面的实验中，您将执行驱逐策略。对于这个实验，您只需要实现SeqScan操作符使用的构造函数和BufferPool.getPage()方法。BufferPool应该存储最多numPages的页面。对于本实验，如果对不同的页面发出超过numPages请求，那么您可能会抛出dbeexception，而不是实现驱逐策略。在未来的实验中，你将被要求执行驱逐政策。

*Database类提供了一个静态方法Database. getbufferpool()，该方法为整个SimpleDB流程返回对单个BufferPool实例的引用。*



Exercise 3 

实现 ->

> src/java/simpledb/storage/BufferPool.java

我们没有为BufferPool提供单元测试。您实现的功能将在下面的HeapFile实现中进行测试。你应该使用`DbFile.readPage`方法访问DbFile的页面。



## BufferPool.java

```java
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 默认的容量设置;
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages; //  表示当前缓存池的容量

    private int requestForPage; // 请求 计数器，表示当前收到了多少 "不同" 页的请求

    private HashMap<PageId,Page> pageMap; // 根据 PageId 和 Page 做映射


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageMap = new HashMap<>();
        this.requestForPage = 0;
    }

    public static int getPageSize() {
        return pageSize;
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
        Page page = pageMap.get(pid);
        if(page != null)  return page;
        requestForPage++; // 如果是不同页的请求, 那么request++
        if(requestForPage > numPages){
            throw new DbException("Lab1: Too much Request for Buffer Pool");
        }
        page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pageMap.put(page.getId(), page); // 要记得重新放回去;
        return page;
    }

}
```





# HeapFile access method

访问方法提供了一种从以特定方式排列的磁盘中读取或写入数据的方法。常用的访问方法包括堆文件(元组的未排序文件)和b -树; 对于lab1，您将只实现一个堆文件访问方法；

HeapFile对象**由一组page构成**，每个页面由**固定数量**的字节组成（`BufferPool.DEFAULT_PAGE_SIZE`），用于存储元组。包括一个头。

在SimpleDB中，数据库中的每个表都有一个HeapFile对象。

HeapFile中的每个页面都被安排为一组插槽，每个插槽可以容纳一个元组。

除了这些槽，每个页面都有一个头，头由一个bitmap组成，每个slot有一个位。如果特定元组对应的位为1，则表示该元组有效；如果它是0，则元组无效(例如，已被删除或从未初始化)。

HeapFile对象的Page类型为HeapPage，它实现了Page接口。页面存储在缓冲池中，但由HeapFile类进行读写。

SimpleDB在磁盘上存储堆文件的格式与存储在内存中的格式大致相同。每个文件由连续排列在磁盘上的页数据组成。每个页面包含一个或多个表示标题的字节，后面是实际页面内容的页面大小字节。每个元组的内容要求元组大小* 8位，头部要求元组大小为1位。因此，在一个页面中可以容纳的元组的数量是:

```java
tuples per page = floor((page size * 8) / (tuple size * 8 + 1))
```

其中元组大小是页面中元组的大小(以字节为单位)。这里的想法是：每个元组在PageHeader的bitmap中需要一个额外的存储位。我们计算页面中的比特数(通过将页面大小乘以8)，并将这个数量除以元组中的比特数(包括bitmap中的标志位)，以得到每页的元组数。**floor操作向下舍入到元组的整数** (我们不希望在页面上存储部分元组!)

一旦我们知道了每页元组的数量，存储头文件所需的字节数很简单：

```java
header bytes = ceiling(tuples per page / 8)
```

ceiling操作四舍五入到最接近的整数字节数(我们存储的头信息绝不会少于一个完整字节)。每个字节的低位(最不重要的位)表示文件中较早的槽的状态。因此，第一个字节的最低位表示页面中的第一个槽是否正在使用。第一个字节的第二个最低位表示页面中的第二个插槽是否正在使用，等等。<u>另外，请注意，最后一个字节的高阶位可能并不对应于文件中实际存在的槽，因为槽的数量可能不是8的倍数</u>。**还要注意，所有Java虚拟机都是大端的。**



Exercise 4

实现下列类：

> -  src/java/simpledb/storage/HeapPageId.java 
> - src/java/simpledb/storage/RecordId.java 
> - src/java/simpledb/storage/HeapPage.java

代码应该通过HeapPageIdTest、RecordIDTest和HeapPageReadTest中的单元测试。

在实现了HeapPage之后，您将在本实验中为HeapFile编写方法，以计算文件中的页面数并从文件中读取页面。然后，您将能够从存储在磁盘上的文件中获取元组。



## HeapPageID.java

```java
public class HeapPageId implements PageId {

    private int tableId;

    private int pgNo;

    /**
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pgNo = pgNo;
    }
}
```



## RecordId.java

```java
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pid;
    private int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }
}
```





## HeapPage.java

```java
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header; // bitmap
    final Tuple[] tuples; // 真正存储tuple的地方
    final int numSlots; // 槽的容量
    final List<Integer> tupleList; // 有效tuple的index集合


    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;


    /** 计算页面容量 */
    private int getNumTuples() {
        return (int) Math.floor((BufferPool.getPageSize() * 8.0 ) / (td.getSize() * 8 + 1));
    }

    /** 计算bitmap大小 */
    private int getHeaderSize() {
        return (int) Math.ceil(getNumTuples() / 8.0);
    }

    /** 返回空闲槽的数量; */
    public int getNumUnusedSlots() {
        int res = 0, index = -1;
        for (int i = 0; i < getNumTuples(); i++) {
            if(i % 8 == 0) index++;
            if(getBit(header[index], i % 8) == 0)
                res++;
            else if (!tupleList.contains(i)){
                tupleList.add(i);
            }
        }
        return res;
    }

    /** 返回某一个槽是否被使用; */
    public boolean isSlotUsed(int i) {
        int index = i / 8;
        return getBit(header[index], i % 8) == 1;
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

    /* Flips the Nth bit in X. */
    private byte flipBit(byte x, int n) {
        /* YOUR CODE HERE */
        int origin = getBit(x,n);
        int flip = (~origin) & 1;
        return setBit(x,n,flip);
    }
}
```



---

Exercise 5

实现 ->

> src/java/simpledb/storage/HeapFile.java

要从磁盘读取一个页面，首先需要计算文件中的正确偏移量。提示:您将需要随机访问文件，以便以任意偏移量读写页面。从磁盘读取page时，不应该调用`BufferPool.getPage`方法。

您还需要实现HeapFile.iterator()方法，该方法应该遍历HeapFile中每个页面的元组。迭代器必须使用BufferPool.getPage()方法来访问HeapFile中的页面。

此方法将页面加载到缓冲池中，并最终(在稍后的实验中)用于实现基于锁定的并发控制和恢复。不要在open()调用时将整个表加载到内存中——这将导致非常大的表造成内存不足错误。



## HeapFile.java

```java
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    
    // 因为一表一个HeapFile, 所以放心使用一个tableId表示一个Heapfile
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.tableId = f.getAbsoluteFile().hashCode();// 需要我们自己生成一个
    }

    // 根据 PageId随机读取一个page,需要先计算page在HeapFile上的偏差,也就是起始位置
    public Page readPage(PageId pid) {
        // 如果tableId不对,那么返回null
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

    /**
     * 返回Heapfile有多少page
     */
    public int numPages() {
        return (int) Math.floor(f.length() * 1.0 / BufferPool.getPageSize());
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

        // 预取下一个page;
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
```





# Operator

操作符负责查询计划的实际执行。它们实现关系代数的操作。在SimpleDB中，操作符是基于迭代器的; 每个操作符实现DbIterator接口。

通过将低级操作符传递给高级操作符的构造函数，即“将它们链接在一起”，操作符被连接到一个计划中。位于计划左侧的特殊访问方法操作符负责从磁盘读取数据(因此在它们下面没有任何操作符)。

在计划的顶部，**与SimpleDB交互的程序只是在根操作符上调用getNext()；该操作符然后在其子操作符上调用getNext()，依此类推，直到调用这些叶操作符**。它们从磁盘获取元组并将它们传递到树中(作为getNext()的返回参数)；元组以这种方式在计划中向上传播，直到它们在根节点上输出，或者被计划中的另一个操作符组合或拒绝。

对于实现INSERT和DELETE查询的计划，最上面的操作符是一个特殊的INSERT或DELETE操作符，用于修改磁盘上的页面。这些操作符将一个元组返回给用户级程序，该元组包含受影响元组的计数。

对于这个实验，您只需要实现一个SimpleDB操作符。



Exercise 6

实现这个类：

> src/java/simpledb/execution/SeqScan.java

该操作符从构造函数中的tableid指定的表页中依次扫描所有元组。该操作符应该通过DbFile.iterator()方法访问元组。

代码应该能够通过ScanTest系统测试。



## Seqscan.java

```java
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableid;
    private String tableAlias;

    private DbFileIterator iterator; // 必须作为类成员, 每个Seqscan一个；
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }


    /**
     * 修改TupleDesc的地方,现在每个字段的名字需要加上 表的别名
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        Iterator<TupleDesc.TDItem> iterator = tupleDesc.iterator();
        Type[] typeAr = new Type[tupleDesc.numFields()]; // 注意这里不是getSize(), 应该是 numFields()
        String[] fieldAr = new String[tupleDesc.numFields()];
        int index = 0;
        while (iterator.hasNext()){
            TupleDesc.TDItem item = iterator.next();
            String fieldNameWithAlias = tableAlias + "." + item.fieldName;
            typeAr[index] = item.fieldType;
            fieldAr[index] = fieldNameWithAlias;
            index++;
        }
        return new TupleDesc(typeAr,fieldAr);
    }

    
    public void open() throws DbException, TransactionAbortedException {
        // 一定要先赋值, 确保 接下来所有 iterator 的调用是同一个 !!!
        this.iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        iterator.open();
    }
    
    
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // 注意这里不要再去获取新的iterator了, 就用自己的iterator
        if(iterator == null) return false;
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if(iterator == null) 
            throw new DbException("ERROR: SeqScan iterator didn't open.");
        return iterator.next();
    }
}
```























