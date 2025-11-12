package org.epdb.engine.comparison;

public enum Op {
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    LESS_THAN,
    GREATER_THAN_OR_EQUALS,
    LESS_THAN_OR_EQUALS;

    @Override
    public String toString() {
        return switch (this) {
            case EQUALS -> "=";
            case NOT_EQUALS -> "!=";
            case GREATER_THAN -> ">";
            case LESS_THAN -> "<";
            case GREATER_THAN_OR_EQUALS -> ">=";
            case LESS_THAN_OR_EQUALS -> "<=";
        };
    }
}
