package org.epdb.app

import org.epdb.catalog.Catalog
import org.epdb.catalog.CatalogModule
import org.epdb.commons.engine.ColumnDefinition
import org.epdb.commons.engine.ColumnType
import org.epdb.commons.engine.Schema
import org.epdb.engine.EngineModule
import org.epdb.engine.queryexecutor.InsertQueryExecutor
import org.epdb.org.epdb.commons.Logger

fun main(args: Array<String>) {
    Logger.info("*************************************************")
    Logger.info("** Database System Startup **")
    Logger.info("*************************************************")

    val tableName = "users"
    val catalog = setCatalogSchema(tableName)
    val db = EngineModule.queryExecutor

    if(args.isNotEmpty() && args.contains("--verify")) {
        Logger.info("* Verification mode enabled.")
        Logger.disabled()
        val insertQueryExecutor = InsertQueryExecutor(catalog)
        insertQueryExecutor.populateTestData("users")
        val result = db.executeSelectQueryWithFilterAndProjection(5099, setOf(0, 1))
        assert(result.size == 1)
    } else {
        val insertQueryExecutor = InsertQueryExecutor(catalog)
        insertQueryExecutor.populateTestData("users")
        db.executeSelectQueryWithFilterAndProjection(5099, setOf(0, 1))
    }
}

private fun setCatalogSchema(tableName: String): Catalog {
    val catalog = CatalogModule.catalog
    val idColumn = ColumnDefinition("id", ColumnType.INT)
    val nameColumn = ColumnDefinition("name", ColumnType.STRING_FIXED_TYPE)
    val ageColumn = ColumnDefinition("age", ColumnType.INT)
    catalog.setTableSchema(tableName, Schema(listOf(idColumn, nameColumn, ageColumn)))
    return catalog
}