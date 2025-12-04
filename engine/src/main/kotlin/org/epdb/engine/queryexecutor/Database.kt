package org.epdb.engine.queryexecutor

import org.epdb.buffer.BufferManager
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.comparison.ComparisonPredicate
import org.epdb.engine.comparisonoperator.ComparisonOperator
import org.epdb.engine.databaseoperator.Projection
import org.epdb.engine.databaseoperator.Selection
import org.epdb.engine.databaseoperator.TableScan
import org.epdb.engine.dto.Schema
import org.epdb.engine.dto.Tuple
import org.epdb.engine.databaseoperator.IndexScan
import org.epdb.engine.databaseoperator.Insert
import org.epdb.engine.databaseoperator.Operator
import org.epdb.index.IndexManager
import org.epdb.storage.manager.StorageManager

class Database(
    private val bufferManager: BufferManager,
    private val storageManager: StorageManager,
    private val indexManager: IndexManager
) {
    private val schema: Schema

    init {
        repeat(3) { storageManager.allocatePage() }
        this.schema = Schema(mutableListOf("id", "name", "age"))
        println("Database initialized and ready.")
    }

    companion object {
        private const val USERS_TABLE_START_PAGE = 0L
    }

    fun executeSelectQuery(tableName: String) {
        if (!checkTableName(tableName)) {
            return
        }

        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1L
        val scanOperator = TableScan(bufferManager, schema, USERS_TABLE_START_PAGE, tablePageCount)

        executeAndPrint(scanOperator, "SELECT * FROM users")
    }

    fun executeSelectQueryWithFilter(tableName: String) {
        if (!checkTableName(tableName)) {
            return
        }

        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1L
        val scanOperator = TableScan(bufferManager, schema, USERS_TABLE_START_PAGE, tablePageCount)
        val predicate = ComparisonPredicate(0, ComparisonOperator.GREATER_THAN, IntValue(102))
        val filterOperator = Selection(predicate, scanOperator)

        executeAndPrint(filterOperator, "SELECT * FROM users")
    }

    fun executeSelectQueryWithFilterAndProjection(tableName: String) {
        if (!checkTableName(tableName)) return

        val indexScanOperator = IndexScan(bufferManager, indexManager, schema, IntValue(5099))
        val predicate = ComparisonPredicate(0, ComparisonOperator.EQUALS, IntValue(5099))
        val filterOperator = Selection(predicate, indexScanOperator)
        val projectionOperator = Projection(filterOperator, setOf(0, 1))

        executeAndPrint(projectionOperator, "SELECT id, name FROM users WHERE id = 5099 (via IndexScan)")
    }

    fun executeInsert(tupleToInsert: Tuple) {
        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1L
        val insertOperator = Insert(
            bufferManager,
            tupleToInsert,
            tablePageCount,
            USERS_TABLE_START_PAGE,
            indexManager
        )

        insertOperator.use { op ->
            op.open()
            op.next()
        }
    }

    fun populateTestData() {
        println("\n--- Admin: Populating Test Data Directly to Storage ---")
        val rowCount = 5000

        for (i in 0..<rowCount) {
            val id = IntValue(i + 100)
            val name = StringValue("User_$i")
            val age = IntValue(20 + i)

            executeInsert(Tuple(listOf(id, name, age)))
        }
        println("--- Admin: Data population finished. ---")
    }

    private fun executeAndPrint(rootOperator: Operator, queryDescription: String) {
        println("\n--- Query Execution: $queryDescription ---")
        println(schema.columnNames)
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
