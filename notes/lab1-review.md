tuple 的构造方法不需要set field，只需要创建List就好了

```java
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
```



这个实现是有错的：

```java
    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */    
public void resetTupleDesc(TupleDesc td) {
        Tuple newTuple = new Tuple(td);
        this.fields = newTuple.fields;
        this.tupleDesc = newTuple.tupleDesc;
        // recordId 不变
    }
```

