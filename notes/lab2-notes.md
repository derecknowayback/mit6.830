6.5830/6.5831 Lab 2: SimpleDB Operators

# Get Started

在这个实验作业中，您将为SimpleDB编写一组操作符，以实现表修改(例如，插入和删除记录)、选择、连接和聚合。它们将建立在您在实验室1中编写的基础之上，为您提供一个可以对多个表执行简单查询的数据库系统。

此外，我们在实验1中忽略了缓冲池管理的问题:我们没有处理在数据库生命周期中引用的页面超过内存容量时所产生的问题。在实验2中，您将设计一个清除策略来从缓冲池中清除过期页面。

在这个实验中，您不需要实现事务或锁定。

实验2需要完成以下功能：

- 实现操作符Filter和Join，并验证它们对应的测试是否有效。我们已经为您提供了Project和OrderBy的实现，这可能有助于您理解其他操作符的工作方式。
- 实现IntegerAggregator和StringAggregator。在这里，您将编写一个逻辑，该逻辑实际上是跨输入元组序列中的多个组在特定字段上计算一个聚合。<u>使用整数除法计算平均值，因为SimpleDB只支持整数。</u>stringaggregator只需要支持COUNT聚合，因为其他操作对字符串没有意义。
- 实现`Aggregate`运算符。与其他操作符一样，聚合实现了OpIterator接口，因此可以将它们放在SimpleDB查询计划中。请注意，对于每次调用next()，聚合操作符的输出是整个组的聚合值，<u>并且聚合构造函数接受聚合和分组字段</u>。
- 在BufferPool中实现与元组插入、删除和页面取出相关的方法。此时您不需要担心事务。
- 实现`Insert`和`Delete`操作符。像所有操作符一样，Insert和Delete实现了OpIterator，接受元组流进行插入或删除，并输出单个元组，其中包含一个整数字段，表示插入或删除元组的数量。这些操作符需要调用BufferPool中实际修改磁盘上的页面的适当方法。检查插入和删除元组的测试是否通过。


最后，您可能注意到，本实验中的迭代器扩展了Operator类，而不是实现OpIterator接口。因为next / hasNext的实现通常是重复的、烦人的、容易出错的，所以Operator一般地实现了这个逻辑，只要求您实现一个更简单的readNext。您可以随意使用这种实现风格，如果您喜欢，也可以只实现OpIterator接口。要实现OpIterator接口，请从迭代器类中删除extends Operator，并在其位置上实现OpIterator。



# Filter & Join

回想一下SimpleDB OpIterator类实现关系代数的操作。现在您将实现两个操作符，使您能够执行比表扫描更有趣的查询。

Filter: 该操作符只返回满足Predicate的元组，Predicate是其构造函数的一部分。因此，它会过滤掉与谓词不匹配的任何元组。

Join: 该操作符根据作为构造函数一部分传入的JoinPredicate来连接它的两个子元组。我们只需要一个简单的嵌套循环连接，但是您可以探索更多有趣的连接实现。在你的实验报告中描述你的实现。



**Exercise 1**

实现下面这些类：

> src/java/simpledb/execution/Predicate.java src/java/simpledb/execution/JoinPredicate.java src/java/simpledb/execution/Filter.java src/java/simpledb/execution/Join.java

代码应该通过PredicateTest、JoinPredicateTest、FilterTest和JoinTest中的单元测试。此外，代码应该能够通过系统测试FilterTest和JoinTest。





## Pridicate.java

```java
/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 内部枚举类，用于表示不同的操作, 在Field.compare中使用
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private int field;
    private Op op; 
    private Field operand;

    /**
     * @param field   另外一个tuple中field的index.
     * @param op      比较运算符
     * @param operand 要比较的数
     */
    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }


    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t 另外一个tuple
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        return t.getField(field).compare(op,operand);
    }
}
```



## JoinPredicate.java

```java
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int field1;
    private int field2;
    Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); 
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1= field1;
        this.field2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(field1).compare(op,t2.getField(field2));
    }
}
```



## Filter.java

我不确定是不是返回子操作符的tupleDesc，但从道理来说应该是这样的；

```java
/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
    }

	// 返回子操作符的tupleDesc
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open(); // 先 open 自己, open使用super就好
        child.open(); // 再 open 子节点
    }

    public void close() {
        child.close(); // 先关闭孩子
        super.close(); // 再关闭自己
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind(); // 孩子rewind了就好, 因为我们的next是基于子操作符的;
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
       while (child.hasNext()){
           Tuple next = child.next();
           if(p.filter(next))
               return next;
       }
        return null;
    }
}
```







## Join.java

我不是很确定这两个函数的实现有没有错：

```java
    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        int field1 = p.getField1();
        return children[0].getTupleDesc().getFieldName(field1); // TODO buggy!!!
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        int field2 = p.getField2();
        return children[1].getTupleDesc().getFieldName(field2); // TODO buggy!!!
    }
```

目前的测试并没有用到这两个函数，我暂时没想到怎么获取 表名or表别名，Catalog吗？那也需要tableId吧；



```java
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator [] children;

    private Tuple outerTuple; // 这个相当于外表 (outer-table) 的计数器, 说明我们配对到哪里了

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.p = p;
        // 注释规定了 child1是外表， child2是内表
        this.children = new OpIterator[]{child1,child2};
        // 外表的tuple设置为null
        this.outerTuple = null;
    }


    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        int field1 = p.getField1();
        return children[0].getTupleDesc().getFieldName(field1); // TODO buggy!!!
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        int field2 = p.getField2();
        return children[1].getTupleDesc().getFieldName(field2); // TODO buggy!!!
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(children[0].getTupleDesc(), children[1].getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // 老样子，先开自己，再开外表、内表
        super.open();
        children[0].open();
        children[1].open();
    }

    public void close() {
        // 先关孩子,再关自己
        children[0].close();
        children[1].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
       children[0].rewind();
       outerTuple = null; // 记得这里要重新设置为 null
       children[1].rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple tuple1 = null, tuple2 = null;
        if(outerTuple == null){
            if(children[0].hasNext())
                outerTuple = children[0].next();
            else return null; // outer 表没有结果, 直接返回 null
        }
        // nested-loop
        while (true) {
            boolean hasFound = false;
            // 循环整个内表，和当前的outerTuple比较
            while (children[1].hasNext()){
                tuple1 = outerTuple;
                tuple2 = children[1].next();
                // 如果满足条件
                if(p.filter(tuple1,tuple2)){
                    hasFound = true;
                    break;
                }
            }
            if(hasFound) break;
            // 到了这里说明上面都没有找到, 去找下一个outerTuple
            if(children[0].hasNext()){
                outerTuple = children[0].next(); // outer-table 下一条
                children[1].rewind(); // 别忘了rewind inner-table
            }
            else
                return null; // 说明 outer 表已经遍历完了
        }
        // 接下来只需要简单拼接就好了,用新的tupleDesc
        int len1 = tuple1.getTupleDesc().numFields(), len2 = tuple2.getTupleDesc().numFields();
        Tuple res = new Tuple(getTupleDesc()); // 利用新的tupleDesc
        for (int i = 0; i < len1; i++) {
            res.setField(i, tuple1.getField(i));
        }
        for (int i = len1; i < len1 + len2; i++) {
            res.setField(i, tuple2.getField(i - len1));
        }
        return res;
    }

}
```



# Aggregates

另一个SimpleDB操作符使用GROUP BY子句实现基本的SQL聚合。您应该实现5个SQL聚合(COUNT、SUM、AVG、MIN、MAX)并支持分组。您只需要支持单个字段的聚合和按单个字段分组。

为了计算聚合，我们使用Aggregator接口，它将一个新的元组合并到现有的聚合计算中。在构造过程中，Aggregator被告知应该使用什么操作进行聚合。

随后，客户端代码应该为子迭代器中的每个元组调用Aggregator.mergeTupleIntoGroup()。所有元组合并后，客户机可以检索聚合结果的OpIterator。结果中的每个元组都是形式(groupValue, aggregateValue)的一对，除非分组字段的值是Aggregator.NO_GROUPING，在这种情况下，结果是表单(aggregateValue)的单个元组。

注意，这种实现要求不同组的数量呈线性。对于本实验，您不需要担心组的数量超过可用内存的情况。



**Exercise 2**

> src/java/simpledb/execution/IntegerAggregator.java 
>
> src/java/simpledb/execution/StringAggregator.java 
>
> src/java/simpledb/execution/Aggregate.java

代码应该通过单元测试IntegerAggregatorTest、StringAggregatorTest和AggregateTest。此外，您应该能够通过AggregateTest系统测试。



## IntegerAggregator.java

这次实验指导书说的也不明确，IntegerAggregator到底是指 "分组Filed是IntType" 还是 "聚合Filed是IntType"，看了测试类才知道 IntegerAggregator 是指聚合类是IntType；

```java
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field emptyFiled = new IntField(-1);  // 空filed，负责no-group的时候统一聚合

    private int gbfield;
    private int afield;
    private Op what;

    private TupleDesc aggSchema; // aggSchema 新的tupleDesc

    private HashMap<Field,List<Integer>> aggregation; // 保存某一个聚类的所有值

    private HashMap<Field,Integer> aggResult; // 保存聚合的结果

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;
        this.aggregation = new HashMap<>();
        this.aggResult = new HashMap<>();
        // 生成聚合schema
        Type[] types = null;
        if(gbfield == NO_GROUPING)
            types = new Type[]{Type.INT_TYPE};
        else
            types = new Type[]{gbfieldtype,Type.INT_TYPE};
        this.aggSchema = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = null;
        if(gbfield != NO_GROUPING)
            field = tup.getField(gbfield);
        else
            field = emptyFiled; // 如果没有分类的话, 就一直用这个emptyFiled
        int value = ((IntField)tup.getField(afield)).getValue();
        List<Integer> list = aggregation.get(field);
        if(list != null) list.add(value);
        else {
            list = new ArrayList<>();
            list.add(value);
            aggregation.put(field,list);
        }
        // 开始计算聚合
        // MIN, MAX, SUM, AVG, COUNT,
        switch (what) {
            case MIN:
                doMin(field, value);
                break;
            case MAX:
                doMax(field, value);
                break;
            case AVG:
                doAVG(field);
                break;
            case SUM:
                doSum(field, value);
                break;
            case COUNT:
                doCount(field);
                break;
        }
    }

    // 新发现的lambda型api，merge的写法很优雅
    private void doMax(Field f, int value) {
        aggResult.merge(f, value, Math::max);
    }

    private void doCount(Field f) {
        aggResult.put(f,aggregation.get(f).size());
    }

    private void doSum(Field f, int value) {
        aggResult.merge(f,value, Integer::sum);
    }

    // avg是最麻烦的，需要我们保存所有的数据，重新遍历再求结果 ;
    // (注意： 不能只保存上一次的avg，然后用 newAvg = (oldAvg * (size - 1) + value) / size，
    // 不能这样写的原因是：我们做的是整数除法，每次都会有偏差，所以之后偏差会越来越多)
    private void doAVG(Field f) {
        List<Integer> integers = aggregation.get(f);
        int sum = 0, avg;
        for (int k : integers) {
            sum += k;
        }
        avg = sum / integers.size();
        aggResult.merge(f,avg,(a,b) ->avg);
    }

    private void doMin(Field f,int value) {
        aggResult.merge(f, value, Math::min);
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntAggIterator();
    }

    private class IntAggIterator implements OpIterator{

        private List<Tuple> aggTuples;

        private Iterator<Tuple> tupleIterator;


        @Override
        public void open() throws DbException, TransactionAbortedException {
            aggTuples = new ArrayList<>();
            for (Field f: aggResult.keySet()){
                int value = aggResult.get(f);
                Tuple tuple = new Tuple(aggSchema);
                if(gbfield == NO_GROUPING){
                    tuple.setField(0,new IntField(value));
                }else{
                    tuple.setField(0,f);
                    tuple.setField(1,new IntField(value));
                }
                aggTuples.add(tuple);
            }
            tupleIterator = aggTuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggSchema;
        }
}
```

这里AVG耽误了一会时间，原先自作聪明只存旧的avg，后来发现**整数除法不靠谱**，就会和答案出现偏差；



## StringAggregator.java 

和IntAggregator差不多，只不过因为聚合Field是String，所以只需要支持count就好了；

```java
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field emptyFiled = new StringField("",1);

    private int gbfield;

    private Op what;

    private TupleDesc aggSchema;

    private HashMap<Field,Integer> aggregation;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    // String 只需要支持 COUNT 就可以了
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // 如果 Op 不是count,那么就抛出异常;
        if(what != Op.COUNT) throw new IllegalArgumentException();
        this.gbfield = gbfield;
        // 我们只关心count，所以具体的value值是什么我们不关心了，所以不需要afield
        this.what = what;
        this.aggregation = new HashMap<>();
        // 生成聚合schema
        Type[] types = null;
        if(gbfield == NO_GROUPING)
            types = new Type[]{Type.INT_TYPE};
        else
            types = new Type[]{gbfieldtype,Type.INT_TYPE};
        this.aggSchema = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = null;
        if(gbfield != NO_GROUPING)
            field = tup.getField(gbfield);
        else
            field = emptyFiled; // 如果没有分类的话, 就一直用这个emptyFiled
        aggregation.merge(field,1,(a,b) -> (a + 1)); // count ++
    }


    public OpIterator iterator() {
        return new StringAggIterator();
    }

    private class StringAggIterator implements OpIterator{

        List<Tuple> tupleList;
        Iterator<Tuple> iterator;
        
		// ... 剩下的和Int一样
    }
}
```



## Aggregate.java

```java
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private Aggregator aggregator;

    private int gfield;
    private int afield;
    private String groupFieldName;
    private String aggregateFieldName;

    private Aggregator.Op aop;

    private OpIterator iterator;
    private OpIterator child;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.afield = afield;
        aggregateFieldName = child.getTupleDesc().getFieldName(afield);
        this.gfield =gfield;
        this.groupFieldName = null;
        this.aop = aop;
        this.child = child;
        Type aggType = child.getTupleDesc().getFieldType(afield), groupType = null;
        if(gfield != Aggregator.NO_GROUPING){
            groupType = child.getTupleDesc().getFieldType(gfield);
            groupFieldName = child.getTupleDesc().getFieldName(gfield);
        }
        if(aggType == Type.INT_TYPE){
            aggregator = new IntegerAggregator(gfield,groupType,afield,aop);
        }else{
            aggregator = new StringAggregator(gfield,groupType,afield,aop);
        }
    }


    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open(); // 要调用父类的open!!!
        child.open(); // 一定要先open子节点才可以hasNext!!!
        int average = 0, count = 0, sum = 0;
        // 这个时候子节点已经做完了所有工作了，我们要拿到子节点的所有tuple;
        while (child.hasNext()){
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        OpIterator real = aggregator.iterator();
        real.open();
        iterator = real;
    }


    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = child.getTupleDesc();
        int numFields = gfield == Aggregator.NO_GROUPING ? 1 : 2;
        Type[] types = new Type[numFields];
        String[] names = new String[numFields];
//        String aggName = aggregateFieldName + "(" + aop + ")("  + child.getTupleDesc() + ".getFieldName(" + afield + "))";
        String aggName = aggregateFieldName;
        if(numFields == 2){
            types[0] = tupleDesc.getFieldType(gfield);
            names[0] = groupFieldName;
        }
        types[numFields - 1] = tupleDesc.getFieldType(afield);
        names[numFields - 1] = aggName;
        return new TupleDesc(types,names);
    }

    public void close() {
        child.close();
        super.close();
        iterator.close();
    }
}
```



主要问题出在下面这个函数，注释的3个点在debug的时候很困扰：

```java
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open(); // 要调用父类的open!!! 不然测试不通过;
        child.open(); // 一定要先open子节点才可以hasNext!!!
        int average = 0, count = 0, sum = 0;
// 这个时候子节点已经做完了所有工作了，我们要拿到子节点的所有tuple;(不这样做会没有tuple,我们需要自己从子节点拿tuple)
        while (child.hasNext()){
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        OpIterator real = aggregator.iterator();
        real.open();
        iterator = real;
    }
```



还有下面这个注释，不能按注释这样改aggField的name，不能修改column-name，按照注释做反而会出错：

> The name of an aggregate column should be informative. For example: "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given in the constructor, and child_td is the TupleDesc of the child iterator.