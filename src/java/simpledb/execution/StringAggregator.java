package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
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

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringAggIterator();
    }

    private class StringAggIterator implements OpIterator{

        List<Tuple> tupleList;
        Iterator<Tuple> iterator;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            tupleList = new ArrayList<>();
            for (Field f: aggregation.keySet()) {
                int size = aggregation.get(f);
                Tuple tuple = new Tuple(aggSchema);
                if(gbfield == NO_GROUPING)
                    tuple.setField(0,new IntField(size));
                else {
                    tuple.setField(0,f);
                    tuple.setField(1,new IntField(size));
                }
                tupleList.add(tuple);
            }
            iterator = tupleList.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(iterator == null) return false;
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = tupleList.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggSchema;
        }

        @Override
        public void close() {
            tupleList.clear();
        }
    }

}
