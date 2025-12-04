package org.epdb.engine.comparisonoperator

enum class ComparisonOperator(val symbol: String) : Op {
    EQUALS("=") {
        override fun apply(comparison: Int): Boolean  = comparison == 0
    },
    NOT_EQUALS("!=") {
        override fun apply(comparison: Int): Boolean  = comparison != 0
    },
    GREATER_THAN(">") {
        override fun apply(comparison: Int): Boolean  = comparison > 0
    },
    LESS_THAN("<") {
        override fun apply(comparison: Int): Boolean  = comparison < 0
    },
    GREATER_THAN_OR_EQUALS(">=") {
        override fun apply(comparison: Int): Boolean  = comparison >= 0
    },
    LESS_THAN_OR_EQUALS("<=") {
        override fun apply(comparison: Int): Boolean  = comparison <= 0
    };

    override fun toString(): String = symbol
}