package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
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

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        if (typeList == null) return null; // 习惯检查, 其实没有必要
        return typeList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
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

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr,null); // 直接调用上一个构造函数;
    }

    public TupleDesc() {
        this.typeList = new ArrayList<>();
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        if (typeList == null) return -1;
        return typeList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || typeList == null || i >= typeList.size()) {
            throw new NoSuchElementException();
        }
        return typeList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || typeList == null || i >= typeList.size()) {
            throw new NoSuchElementException();
        }
        return typeList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
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
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
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
     * @param i 目标item的下标
     * @return 目标item
     */
    public TDItem getItem(int i) {
        int len;
        if (typeList == null || (len = typeList.size()) == 0 || i < 0 || i >= len) return null;
        return typeList.get(i);
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
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
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc another = (TupleDesc) o;
        if (this.numFields() != another.numFields()) return false;
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getItem(i).equals(another.getItem(i)))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Objects.hash(typeList);
    }


    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
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
