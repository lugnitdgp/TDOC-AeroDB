package com.aerodb.storage;

import java.util.ArrayList;
import java.util.List;

public class TupleDesc {
    private List<Type> types;
    private List<String> fieldNames;

    public TupleDesc() {
        this.types = new ArrayList<>();
        this.fieldNames = new ArrayList<>();
    }

    public void addField(Type type, String name) {
        types.add(type);
        fieldNames.add(name);
    }

    public int numFields() {
        return types.size();
    }

    public Type getType(int i) {
        return types.get(i);
    }
}
