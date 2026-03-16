package org.epdb.engine.columntypes

data class StringValue(val value: String) : ColumnValue {

    override fun comparesTo(other: ColumnValue): Int {
        return when (other) {
            is StringValue -> {
                this.value.compareTo(other.value)
            }

            else -> throw ClassCastException("Cannot compare IntValue with ${other.javaClass.getSimpleName()}")
        }
    }
}