package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
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

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        return groupFieldName;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return aggregateFieldName;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open(); // ????????????open??????????????????hasNext
        // ???????????????????????????????????????????????????????????????????????????????????????tuple;
        while (child.hasNext()){
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        OpIterator real = aggregator.iterator();
        real.open();
        iterator = real;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(iterator.hasNext()) return iterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
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

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
