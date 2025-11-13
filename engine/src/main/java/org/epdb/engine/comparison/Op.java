package org.epdb.engine.comparison;

public enum Op {
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    LESS_THAN,
    GREATER_THAN_OR_EQUALS,
    LESS_THAN_OR_EQUALS;

    public boolean apply(int comparison) {
        return switch (this) {
            case EQUALS -> comparison == 0;
            case LESS_THAN -> comparison < 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_THAN_OR_EQUALS -> comparison >= 0;
            case LESS_THAN_OR_EQUALS -> comparison <= 0;
            case NOT_EQUALS -> comparison != 0;
        };
    }

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
