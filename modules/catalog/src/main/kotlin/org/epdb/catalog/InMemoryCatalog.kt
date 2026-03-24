package org.epdb.catalog

import org.epdb.commons.engine.Schema

class InMemoryCatalog(
    private val catalogMap: MutableMap<String, TableCatalog>
) : Catalog {

    override fun getTableSchema(tableName: String) : Schema? {
        return catalogMap[tableName]?.tableSchema
    }

    override fun setTableSchema(tableName: String, schema: Schema) {
        catalogMap[tableName] = TableCatalog(
            tableId = catalogMap.size + 1,
            tableSchema = schema,
            pageIds = mutableListOf()
        )
    }

    override fun removeTableSchema(tableName: String) {
       catalogMap.remove(tableName)
    }

    override fun addPageIdToTable(tableName: String, pageId: Long) {
        catalogMap[tableName]?.pageIds?.add(pageId)
    }
}