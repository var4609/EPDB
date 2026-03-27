package org.epdb.engine.queryexecutor

import org.epdb.catalog.Catalog
import org.epdb.commons.engine.ColumnType
import org.epdb.engine.EngineModule
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.dto.Tuple
import org.epdb.org.epdb.commons.Logger

class InsertQueryExecutor(private val catalog: Catalog) {

    fun insertData(tableName: String, tuple: List<String>) {
        catalog.getTableSchema(tableName)?.let { schema ->

            assert(tuple.size == schema.columnDefinitions.size) {
                Logger.error("Bad row being inserted, num columns do not match")
            }

            val tuple = tuple.zip(schema.columnDefinitions).map { (data, columnDef) ->
                when (columnDef.columnType) {
                    ColumnType.INT -> { ColumnValue.IntValue(data.toInt()) }
                    ColumnType.STRING_FIXED_TYPE -> { ColumnValue.StringValue(data)}
                }
            }

            val insertOperator = EngineModule.createInsertOperator(tableName, Tuple(tuple))

            insertOperator.use { op ->
                op.open()
                op.next()
            }
        }
    }

    fun populateTestData(tableName: String) {
        Logger.info("\n--- Admin: Populating Test Data Directly to Storage ---")
        val rowCount = 5000

        for (i in 0..<rowCount) {
            val id = "${i + 100}"
            val name = "User_$i"
            val age = "${i + 20}"

            insertData(tableName, listOf(id, name, age))
        }
        Logger.info("--- Admin: Data population finished. ---")
    }
}