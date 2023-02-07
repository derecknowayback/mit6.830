package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<Field> fields;

    private TupleDesc tupleDesc;

    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.fields = new ArrayList<>();
        if(td == null || td.numFields() < 1){
            return;
        }
        this.tupleDesc = td;
        int len = td.numFields();
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
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if(fields == null || i >= fields.size() || i < 0 ) return;
        fields.set(i,f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if(fields == null || i >= fields.size() || i < 0 ) return null;
        Field field = fields.get(i);
        return field; // ??? 这不是value啊
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

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        if(fields == null) return null;
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        Tuple newTuple = new Tuple(td);
        this.fields = newTuple.fields;
        this.tupleDesc = newTuple.tupleDesc;
        // recordId 不变
    }
}
