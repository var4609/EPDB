package org.epdb.engine.dto

import org.epdb.engine.columntypes.ColumnValue

data class Tuple(val values: List<ColumnValue>) {

    fun getValueAtIndex(index: Int): ColumnValue {
        return this.values[index]
    }

    override fun toString(): String {

        return values.toString()
    }
}
