package org.epdb.engine.dto;

import java.util.Arrays;

public record Tuple(Object[] values) {

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
