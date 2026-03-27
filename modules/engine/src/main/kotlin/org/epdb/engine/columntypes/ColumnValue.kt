package org.epdb.engine.columntypes

sealed class ColumnValue : Comparable<ColumnValue> {

    data class IntValue(val value: Int) : ColumnValue()
    data class StringValue(val value: String) : ColumnValue()

    override fun compareTo(other: ColumnValue): Int {
        return when (this) {
            is IntValue if other is IntValue -> {
                this.value.compareTo(other.value)
            }

            is StringValue if other is StringValue -> {
                this.value.compareTo(other.value)
            }

            else -> throw ClassCastException("Cannot compare IntValue with ${other.javaClass.getSimpleName()}")
        }
    }
}
