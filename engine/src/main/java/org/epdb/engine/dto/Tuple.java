package org.epdb.engine.dto;

import java.util.Arrays;

public record Tuple(ColumnValue[] values) {

    public ColumnValue getValueAtIndex(int index) {
        return this.values()[index];
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
