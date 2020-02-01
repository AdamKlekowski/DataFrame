package main.dataFrame;

import main.value.Value;

import java.util.ArrayList;

public class Column implements Cloneable {
    public String name;
    public String type;
    public ArrayList<Value> records;

    Column(String name, String type) {
        this.name = name;
        this.type = type;
        records = new ArrayList<>();
    }

    @Override
    public Column clone() throws CloneNotSupportedException {
        Column columnCloned = (Column) super.clone();
        columnCloned.records = new ArrayList<>();
        columnCloned.records.addAll(records);
        return columnCloned;
    }
}