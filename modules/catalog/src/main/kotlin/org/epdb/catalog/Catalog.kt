package org.epdb.catalog

import org.epdb.commons.engine.Schema

interface Catalog {

    fun getTableSchema(tableName: String): Schema?

    fun getRowSizeInBytes(tableName: String): Int

    fun getTablePageIds(tableName: String): List<Long>

    fun setTableSchema(tableName: String, schema: Schema)

    fun removeTableSchema(tableName: String)

    fun addPageIdToTable(tableName: String, pageId: Long)
}