package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    public static void main(String[] args) {
        HashMap<Integer, Integer> hashMap = new HashMap<>();
        hashMap.merge(1,1,(a,b) -> (a + 1));
        hashMap.merge(1,1,(a,b) -> (a + 1));
        hashMap.merge(1,1,(a,b) -> (a + 1));
        System.out.println(hashMap.get(1));
    }


    private static final long serialVersionUID = 1L;

    private static final Field emptyFiled = new IntField(-1);

    private int gbfield;
    private int afield;
    private Op what;

    private TupleDesc aggSchema;

    private HashMap<Field,Integer> aggregation;

    private HashMap<Field,Integer> aggResult;

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
        aggregation.merge(field,1,(a,b) -> (a + 1));
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
                doAVG(field, value);
                break;
            case SUM:
                doSum(field, value);
                break;
            case COUNT:
                doCount(field);
                break;
        }
    }

    private void doMax(Field f, int value) {
        aggResult.merge(f, value, Math::max);
    }

    private void doCount(Field f) {
        aggResult.put(f,aggregation.get(f));
    }

    private void doSum(Field f, int value) {
        aggResult.merge(f,value, Integer::sum);
    }

    private void doAVG(Field f, int value) {
        aggResult.merge(f,value,(a,b) ->{
            int size = aggregation.get(f);
            return ((size - 1) * a + b) / size;
        });
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

        public IntAggIterator(){
            aggTuples = null;
            tupleIterator = null;
        }

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
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null) return false;
            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()) throw new NoSuchElementException();
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            tupleIterator = aggTuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggSchema;
        }

        @Override
        public void close() {
            aggTuples.clear();
            aggTuples = null;
            tupleIterator = null;
        }
    }

}
