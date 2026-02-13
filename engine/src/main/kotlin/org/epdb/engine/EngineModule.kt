package org.epdb.engine

import org.epdb.buffer.BufferModule
import org.epdb.buffer.manager.BufferManager
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.comparison.Predicate
import org.epdb.engine.databaseoperator.*
import org.epdb.engine.dto.Schema
import org.epdb.engine.dto.Tuple
import org.epdb.engine.dto.ColumnDefinition
import org.epdb.engine.dto.ColumnType
import org.epdb.engine.queryexecutor.Database
import org.epdb.index.IndexModule
import org.epdb.index.dto.PagePointer
import org.epdb.index.manager.IndexManager
import org.epdb.storage.StorageModule

object EngineModule {

    private const val USERS_TABLE_START_PAGE = 0L

    private val schema : Schema by lazy {
        val idCol = ColumnDefinition("id", ColumnType.INT)
        val nameCol = ColumnDefinition("name", ColumnType.STRING_FIXED_TYPE)
        val ageCol = ColumnDefinition("age", ColumnType.INT)
        Schema(mutableListOf(idCol, nameCol, ageCol))
    }

    private val pagePointers : List<PagePointer> by lazy {
        mutableListOf()
    }

    val queryExecutor : Database by lazy {
        Database(StorageModule.storageManager)
    }

    private val bufferManager : BufferManager by lazy {
        BufferModule.bufferManager
    }

    private val indexManager : IndexManager by lazy {
        IndexModule.indexManager
    }

    fun createInsertOperator(tableName: String, tupleToInsert: Tuple) : Operator {
        return Insert(
            bufferManager = bufferManager,
            tupleToInsert = tupleToInsert,
            tableStartPageId = USERS_TABLE_START_PAGE,
            maxAllocatedPageCount = StorageModule.storageManager.getAllocatedPageCount() - 1L,
            indexManager = indexManager
        )
    }

    fun createProjectionOperator(tableName: String, childOperator: Operator, projectionColumns: Set<Int>) : Operator {
        return Projection(
            childOperator = childOperator,
            projectionColumns = projectionColumns
        )
    }

    fun createSelectionOperator(tableName: String, childOperator: Operator, predicate: Predicate) : Operator {
        return Selection(
            childOperator = childOperator,
            predicate = predicate
        )
    }

    fun createIndexScanOperator(tableName: String, searchKey: ColumnValue) : Operator {
        return IndexScan(
            bufferManager = bufferManager,
            indexManager = indexManager,
            schema = schema,
            searchKey = searchKey,
            pagePointers = pagePointers,
            currentPagePointerIndex = 0
        )
    }

    fun createTableScanOperator(tableName: String) : Operator {
        return TableScan(
            bufferManager = bufferManager,
            schema = schema,
            tableStartPageId = USERS_TABLE_START_PAGE,
            maxAllocatedPageId = StorageModule.storageManager.getAllocatedPageCount() - 1L
        )
    }
}