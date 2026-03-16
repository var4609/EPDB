package org.epdb.engine.databaseoperator

import org.epdb.engine.comparison.Predicate
import org.epdb.engine.dto.Tuple
import org.epdb.org.epdb.commons.Logger

data class Selection(
    private val predicate: Predicate,
    private val childOperator: Operator
) : Operator {

    override fun open() {
        this.childOperator.open()
        Logger.info("Opened child operator...")
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
        Logger.info("Closed child operator...")
    }
}
