package com.alibaba.middleware.race.sync.model;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by autulin on 6/12/17.
 */
public class Row {
    //    private String schema;  其实不用存的
//    private String table;
    private Character operation;
    private List<Column> columns = new LinkedList<>();

//    public String getSchema() {
//        return schema;
//    }
//
//    public void setSchema(String schema) {
//        this.schema = schema;
//    }
//
//    public String getTable() {
//        return table;
//    }
//
//    public void setTable(String table) {
//        this.table = table;
//    }

    public Character getOperation() {
        return operation;
    }

    public void setOperation(Character operation) {
        this.operation = operation;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "Row{" +
                "operation=" + operation +
                ", columns=" + columns +
                '}';
    }
}
