package org.epdb.engine.dto;

import java.util.Arrays;

public record Tuple(Object[] values) {

    public Object getValueAtIndex(int index) {
        return this.values()[index];
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
