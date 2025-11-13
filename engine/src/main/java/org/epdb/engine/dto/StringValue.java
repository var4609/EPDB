package org.epdb.engine.dto;

public record StringValue(String value) implements ColumnValue {

    @Override
    public int comparesTo(ColumnValue other) {
        if(other instanceof StringValue(String otherString)) {
            return this.value.compareTo(otherString);
        }

        throw new ClassCastException("Cannot compare StringValue with " + other.getClass().getSimpleName());
    }
}
