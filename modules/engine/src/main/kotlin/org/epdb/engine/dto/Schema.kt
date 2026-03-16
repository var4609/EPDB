package org.epdb.engine.dto

data class Schema(val columnDefinitions: List<ColumnDefinition>) {

    val columnNames: List<String> get() = columnDefinitions.map { it.columnName }
}
