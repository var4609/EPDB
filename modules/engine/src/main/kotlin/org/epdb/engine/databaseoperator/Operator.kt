package org.epdb.engine.databaseoperator

import org.epdb.engine.dto.Tuple

interface Operator : AutoCloseable {
    fun open()

    fun next(): Tuple?
}
