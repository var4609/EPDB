package org.epdb.catalog

import org.epdb.commons.engine.Schema

interface Catalog {

    fun getTableSchema(tableName: String): Schema?

    fun setTableSchema(tableName: String, schema: Schema)

    fun removeTableSchema(tableName: String)

    fun addPageIdToTable(tableName: String, pageId: Long)
}