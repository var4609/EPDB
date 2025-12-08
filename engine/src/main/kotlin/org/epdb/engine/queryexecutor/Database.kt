package org.epdb.engine.queryexecutor

import org.epdb.engine.EngineModule
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.comparison.ComparisonPredicate
import org.epdb.engine.comparisonoperator.ComparisonOperator
import org.epdb.engine.databaseoperator.Operator
import org.epdb.engine.dto.Tuple
import org.epdb.storage.manager.StorageManager

class Database(
    private val storageManager: StorageManager
) {

    init {
        repeat(3) { storageManager.allocatePage() }
        println("Database initialized and ready.")
    }

    fun executeSelectQuery(tableName: String) {
        if (!checkTableName(tableName)) {
            return
        }

        val scanOperator = EngineModule.createTableScanOperator(tableName)
        executeAndPrint(scanOperator, "SELECT * FROM users")
    }

    fun executeSelectQueryWithFilter(tableName: String) {
        if (!checkTableName(tableName)) {
            return
        }

        val scanOperator = EngineModule.createTableScanOperator(tableName)
        val predicate = ComparisonPredicate(0, ComparisonOperator.GREATER_THAN, IntValue(102))
        val filterOperator = EngineModule.createSelectionOperator(tableName, scanOperator, predicate)

        executeAndPrint(filterOperator, "SELECT * FROM users")
    }

    fun executeSelectQueryWithFilterAndProjection(tableName: String) {
        if (!checkTableName(tableName)) return

        val indexScanOperator = EngineModule.createIndexScanOperator(tableName, IntValue(5099))
        val predicate = ComparisonPredicate(0, ComparisonOperator.EQUALS, IntValue(5099))
        val filterOperator = EngineModule.createSelectionOperator(tableName, indexScanOperator, predicate)
        val projectionOperator = EngineModule.createProjectionOperator(tableName, filterOperator, setOf(0, 1))

        executeAndPrint(projectionOperator, "SELECT id, name FROM users WHERE id = 5099 (via IndexScan)")
    }

    fun executeInsert(tableName: String, tupleToInsert: Tuple) {
        val insertOperator = EngineModule.createInsertOperator(
            tableName = tableName,
            tupleToInsert = tupleToInsert
        )

        insertOperator.use { op ->
            op.open()
            op.next()
        }
    }

    fun populateTestData(tableName: String) {
        println("\n--- Admin: Populating Test Data Directly to Storage ---")
        val rowCount = 5000

        for (i in 0..<rowCount) {
            val id = IntValue(i + 100)
            val name = StringValue("User_$i")
            val age = IntValue(20 + i)

            executeInsert(tableName, Tuple(listOf(id, name, age)))
        }
        println("--- Admin: Data population finished. ---")
    }

    private fun executeAndPrint(rootOperator: Operator, queryDescription: String) {
        println("\n--- Query Execution: $queryDescription ---")
        println(mutableListOf("id", "name", "age"))
        println("---------------------------------------------")

        rootOperator.use { op ->
            op.open()
            var tuple: Tuple?

            while (op.next().also { tuple = it } != null) {
                println(tuple)
            }
        }
    }

    private fun checkTableName(tableName: String): Boolean {
        if (tableName != "users") {
            System.err.println("Table not found: $tableName")
            return false
        }
        return true
    }
}
