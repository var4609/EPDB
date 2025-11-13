package org.epdb.engine.dto;

public record IntValue(int value) implements ColumnValue {

    @Override
    public int comparesTo(ColumnValue other) {
        if(other instanceof IntValue(int otherInt)) {
            return Integer.compare(this.value, otherInt);
        }

        throw new ClassCastException("Cannot compare IntValue with " + other.getClass().getSimpleName());
    }
}
