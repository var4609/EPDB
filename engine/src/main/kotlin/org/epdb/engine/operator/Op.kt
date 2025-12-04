package org.epdb.engine.operator

interface Op {

    fun apply(comparison: Int) : Boolean
}