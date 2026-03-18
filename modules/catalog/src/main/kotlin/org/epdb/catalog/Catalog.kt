package org.epdb.catalog

import org.epdb.commons.engine.Schema

interface Catalog {

    fun getTableSchema()

    fun setTableSchema(tableName: String, schema: Schema)
}