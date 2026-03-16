package org.epdb.engine.databaseoperator

import org.epdb.engine.dto.Tuple

data class Projection(
    private val childOperator: Operator,
    private val projectionColumns: Set<Int>
) : Operator {

    override fun open() {
        this.childOperator.open()
    }

    override fun next(): Tuple? {
        val tuple = childOperator.next() ?: return null

        return Tuple(
            values = tuple.values.filterIndexed { index, _ -> projectionColumns.contains(index) }
        )
    }

    override fun close() {
        this.childOperator.close()
    }
}
