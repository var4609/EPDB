package org.epdb.catalog

import org.epdb.commons.engine.Schema

data class TableCatalog(
    val tableId: Int,
    val tableSchema: Schema,
    val pageIds: MutableList<Long>
) {
    val rowSizeInBytes: Int get() = tableSchema.columnDefinitions.sumOf { it.columnType.sizeInBytes }
}
