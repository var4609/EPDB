package org.epdb.commons.engine

data class Schema(val columnDefinitions: List<ColumnDefinition>) {

    val columnNames: List<String> get() = columnDefinitions.map { it.columnName }
}
