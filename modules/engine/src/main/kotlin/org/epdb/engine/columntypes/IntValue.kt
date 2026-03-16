package org.epdb.engine.columntypes

data class IntValue(val value: Int) : ColumnValue {

    override fun comparesTo(other: ColumnValue): Int {
        return when (other) {
            is IntValue -> {
                this.value.compareTo(other.value)
            }

            else -> throw ClassCastException("Cannot compare IntValue with ${other.javaClass.getSimpleName()}")
        }
    }
}