package org.epdb.engine.dto

data class Schema(val columnNames: List<String>) {

    val columnCount: Int get() = this.columnNames.size
}
