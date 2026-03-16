package org.epdb.engine.columntypes

sealed interface ColumnValue {

    fun comparesTo(other: ColumnValue): Int
}
