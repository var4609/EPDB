package org.epdb.engine.comparison

import org.epdb.engine.dto.Tuple

interface Predicate {

    fun evaluate(tuple: Tuple): Boolean
}
