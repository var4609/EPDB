package org.epdb.engine.databaseoperator

import org.epdb.engine.comparison.Predicate
import org.epdb.engine.dto.Tuple

data class Selection(
    private val predicate: Predicate,
    private val childOperator: Operator
) : Operator {

    override fun open() {
        this.childOperator.open()
        println("Opened child operator...")
    }

    override fun next(): Tuple? {
        while (true) {
            val tuple = childOperator.next() ?: return null

            if (predicate.evaluate(tuple)) {
                return tuple
            }
        }
    }

    override fun close() {
        this.childOperator.close()
        println("Closed child operator...")
    }
}
