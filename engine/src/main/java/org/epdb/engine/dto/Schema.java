package org.epdb.engine.dto;

public record Schema(String[] columnNames) {

    public int getColumnCount() {
        return this.columnNames.length;
    }
}
