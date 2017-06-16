package com.alibaba.middleware.race.sync.model;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by autulin on 6/12/17.
 */
public class Row {
    //    private boolean valid;
    private Character operation;
    private List<Column> columns = new LinkedList<>();

//    public boolean isValid() {
//        return valid;
//    }
//
//    public void setValid(boolean valid) {
//        this.valid = valid;
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
