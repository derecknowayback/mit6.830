package simpledb.optimizer;

import javafx.util.Pair;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.index.BTreeFile;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */

    private List<Object> histograms;
    private int ioCostPerPage;

    private int tableId;
    private int ntup;
    private TupleDesc tupleDesc;
    private DbFile file;

    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        this.ioCostPerPage = ioCostPerPage;
        this.tableId = tableid;
        file = Database.getCatalog().getDatabaseFile(tableid);
        tupleDesc = this.file.getTupleDesc();
        int numFields = tupleDesc.numFields();
        histograms = new ArrayList<>();
        for(int i=0; i<numFields; i++)
            histograms.add(null);
        DbFileIterator it = file.iterator(new TransactionId());
        HashMap<Integer,MyPair> histogramMap = null;
        try{
            this.ntup = 0;
            histogramMap =  getMaxMin(it);
        }catch(Exception e){
            e.printStackTrace();
            throw new Error();
        }
        for (int i = 0; i < numFields; i++) {
            if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                MyPair myPair = histogramMap.get(i);
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, myPair.getKey(), myPair.getValue());
                histograms.set(i,intHistogram);
            }else {
                histograms.set(i,new StringHistogram(NUM_HIST_BINS));
            }
        }
        try {
            it.rewind();
        }catch (Exception e){
            e.printStackTrace();
        }
        addValue(it);
        it.close();
    }

    private static class MyPair{
        Integer key,value;

        public MyPair(Integer key, Integer value) {
            this.key = key;
            this.value = value;
        }

        public Integer getKey() {
            return key;
        }

        public void setKey(Integer key) {
            this.key = key;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }
    }

    private HashMap<Integer, MyPair> getMaxMin(DbFileIterator it) {
        HashMap<Integer, MyPair> resMap = new HashMap<>();
        try {
            it.open();
            while (it.hasNext()){
                Tuple t = it.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        MyPair pair = resMap.get(i);
                        int temp = ((IntField) t.getField(i)).getValue();
                        if(pair != null){
                            int min = pair.getKey(), max = pair.getValue();
                            if(temp < min) pair.setKey(temp);
                            if(temp > max) pair.setValue(temp);
                        }else{
                            resMap.put(i,new MyPair(temp,temp));
                        }
                    }
                }
                ntup ++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resMap;
    }

    private void addValue(DbFileIterator iterator){
        try {
            iterator.open();
            while (iterator.hasNext()){
                Tuple t = iterator.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        ((IntHistogram)histograms.get(i)).addValue(((IntField)t.getField(i)).getValue());
                    }else{
                        ((StringHistogram)histograms.get(i)).addValue(((StringField)t.getField(i)).getValue());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private Object getHistogram(int field){
        if(histograms.get(field) == null){
            DbFileIterator it = file.iterator(new TransactionId());
            try{
                it.open();
                Type type = file.getTupleDesc().getFieldType(field);
                if(type == Type.STRING_TYPE){
                    StringHistogram hist = new StringHistogram(NUM_HIST_BINS);
                    while(it.hasNext())
                        hist.addValue(((StringField)it.next().getField(field)).getValue());
                    it.close();
                    histograms.set(field, hist);
                    return hist;
                }else{
                    int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
                    List<Integer> toAdd = new ArrayList<>();
                    while(it.hasNext()){
                        Tuple t = it.next();
                        int x = ((IntField)t.getField(field)).getValue();
                        if(x < min) min = x;
                        if(x > max) max = x;
                        toAdd.add(x);
                    }
                    it.close();
                    IntHistogram hist = new IntHistogram(NUM_HIST_BINS, min, max);
                    for (Integer integer : toAdd) hist.addValue(integer);
                    histograms.set(field, hist);
                    return hist;
                }
            }catch(Exception e){
                e.printStackTrace();
                throw new Error();
            }
        }else
            return histograms.get(field);
    }





    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        if(databaseFile instanceof  HeapFile){
            return ioCostPerPage * ((HeapFile) databaseFile).numPages();
        }else if (databaseFile instanceof BTreeFile){
            return ioCostPerPage * ((BTreeFile) databaseFile).numPages();
        }
        return -1;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.floor(ntup * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        Object o = histograms.get(field);
        Type fieldType = Database.getCatalog().getTupleDesc(tableId).getFieldType(field);
        if (fieldType == Type.INT_TYPE){
            IntHistogram histogram = (IntHistogram) o;
            return histogram.avgSelectivity(); // TODO: buggy
        }else{
            StringHistogram histogram = (StringHistogram) o;
            return histogram.avgSelectivity(); // TODO: buggy
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Object o = histograms.get(field);
        Type fieldType = Database.getCatalog().getTupleDesc(tableId).getFieldType(field);
        if (fieldType == Type.INT_TYPE){
            IntHistogram histogram = (IntHistogram) o;
            return histogram.estimateSelectivity(op,((IntField)constant).getValue());
        }else{
            StringHistogram histogram = (StringHistogram) o;
            return histogram.estimateSelectivity(op,((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return ntup;
    }

}
