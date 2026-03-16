package org.epdb.engine.dto

import org.epdb.engine.dto.ColumnDefinition

data class Schema(val columnDefinitions: List<ColumnDefinition>) {

    val columnNames: List<String> get() = columnDefinitions.map { it.columnName }
}
