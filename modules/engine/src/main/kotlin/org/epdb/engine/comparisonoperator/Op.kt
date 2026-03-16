package org.epdb.engine.comparisonoperator

interface Op {

    fun apply(comparison: Int) : Boolean
}