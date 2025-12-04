package org.epdb.engine.queryexecutor

import org.epdb.buffer.BufferManager
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.comparison.ComparisonPredicate
import org.epdb.engine.comparisonoperator.Operator
import org.epdb.engine.databaseoperator.Projection
import org.epdb.engine.databaseoperator.Selection
import org.epdb.engine.dto.Schema
import org.epdb.engine.dto.Tuple
import org.epdb.engine.volcano.*
import org.epdb.index.IndexManager
import org.epdb.storage.manager.StorageManager
import java.util.List

class Database(
    private val bufferManager: BufferManager?,
    private val storageManager: StorageManager,
    private val indexManager: IndexManager?
) {
    private val schema: Schema

    init {
        this.storageManager.allocatePage()
        this.storageManager.allocatePage()
        this.storageManager.allocatePage()
        this.schema = Schema(mutableListOf<String>("id", "name", "age"))
    }

    fun executeSelectQuery(tableName: String) {
        if (tableName != "users") {
            println("Table not found: " + tableName)
            return
        }

        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1
        val scanOperator = TableScan(bufferManager, schema, USERS_TABLE_START_PAGE, tablePageCount)

        println("\n--- Query Execution: SELECT * FROM users ---")
        println(schema.columnNames)
        println("---------------------------------------------")

        scanOperator.open()
        var tuple: Tuple?
        while ((scanOperator.next().also { tuple = it }) != null) {
            println(tuple)
        }

        scanOperator.close()
    }

    fun executeSelectQueryWithFilter(tableName: String) {
        if (tableName != "users") {
            println("Table not found: " + tableName)
            return
        }

        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1
        val scanOperator = TableScan(bufferManager, schema, USERS_TABLE_START_PAGE, tablePageCount)
        val predicate = ComparisonPredicate(0, Operator.GREATER_THAN, IntValue(102))
        val filterOperator = Selection(predicate, scanOperator)

        println("\n--- Query Execution: SELECT * FROM users WHERE id > 150 ---")
        println(schema.columnNames)
        println("---------------------------------------------")

        filterOperator.open()
        var tuple: Tuple?
        while ((filterOperator.next().also { tuple = it }) != null) {
            println(tuple)
        }

        filterOperator.close()
    }

    fun executeSelectQueryWithFilterAndProjection(tableName: String) {
        if (tableName != "users") {
            println("Table not found: " + tableName)
            return
        }

        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1
        val scanOperator = TableScan(bufferManager, schema, USERS_TABLE_START_PAGE, tablePageCount)
        val indexScanOperator = IndexScan(bufferManager, indexManager, schema, IntValue(5099))
        val predicate = ComparisonPredicate(0, Operator.EQUALS, IntValue(5099))
        val filterOperator = Selection(predicate, indexScanOperator)
        val projectionOperator = Projection(filterOperator, setOf(0, 1))

        println("\n--- Query Execution: SELECT id, name FROM users WHERE id = 5099 ---")
        println(schema.columnNames)
        println("---------------------------------------------")

        projectionOperator.open()
        var tuple: Tuple?
        while ((projectionOperator.next().also { tuple = it }) != null) {
            println(tuple)
        }

        projectionOperator.close()
    }

    fun executeInsert(tupleToInsert: Tuple?) {
        val tablePageCount = this.storageManager.getAllocatedPageCount() - 1
        val insertOperator = Insert(
            bufferManager,
            tupleToInsert,
            tablePageCount,
            USERS_TABLE_START_PAGE,
            indexManager
        )

        insertOperator.open()
        insertOperator.next()
        insertOperator.close()
    }

    fun populateTestData() {
        println("\n--- Admin: Populating Test Data Directly to Storage ---")
        val rowCount = 5000

        for (i in 0..<rowCount) {
            val id = IntValue(i + 100)
            val name = StringValue("User_" + i)
            val age = IntValue(20 + i)

            executeInsert(Tuple(List.of<ColumnValue?>(id, name, age)))
        }
    }

    companion object {
        private const val USERS_TABLE_START_PAGE = 0L
    }
}
