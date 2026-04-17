package org.epdb.engine.databaseoperator

import org.epdb.buffer.manager.BufferManager
import org.epdb.catalog.Catalog
import org.epdb.org.epdb.commons.engine.ColumnValue
import org.epdb.engine.dto.Tuple
import org.epdb.index.manager.IndexManager
import org.epdb.org.epdb.commons.Logger
import org.epdb.org.epdb.commons.util.SerializationService
import org.epdb.storage.dto.Page
import java.nio.ByteBuffer
import java.nio.ByteOrder

class Insert(
    private val bufferManager: BufferManager,
    private val catalog: Catalog,
    private val tableName: String,
    private val tupleToInsert: Tuple,
    private val indexManager: IndexManager,
    private var isExecuted: Boolean = false
) : Operator {

    companion object {
        private const val INDEXED_COLUMN_ID = 0
    }

    override fun open() {}

    override fun next(): Tuple? {
        if (this.isExecuted) return null
        this.isExecuted = true

        val serializedTuple = serializeTuple(tupleToInsert)

        catalog.getTablePageIds(tableName).forEach { currentPageId ->
            val page = bufferManager.getPage(currentPageId, tableName)

            try {
                if (canInsertTuple(page, serializedTuple.size)) {
                    updateColumnIndex(tupleToInsert.getValueAtIndex(INDEXED_COLUMN_ID), page.currentNumSlots, page.pageId)
                    page.writeTupleAndSlot(serializedTuple)
                    Logger.info("Inserted tuple: $tupleToInsert")
                    bufferManager.unpinPage(currentPageId, true)
                    return this.tupleToInsert
                }

                bufferManager.unpinPage(currentPageId, false)
            } catch (e: Exception) {
                bufferManager.unpinPage(currentPageId, false)
                throw e
            }
        }

        val newPage = this.bufferManager.allocateNewPage(tableName)
        try {
            updateColumnIndex(tupleToInsert.getValueAtIndex(INDEXED_COLUMN_ID), 0, newPage.pageId)
            newPage.writeTupleAndSlot(serializedTuple)
            Logger.info("Inserted tuple: $tupleToInsert")
            return this.tupleToInsert
        } finally {
            bufferManager.unpinPage(newPage.pageId, true)
        }
    }

    override fun close() {}

    private fun canInsertTuple(page: Page, requiredSize: Int) : Boolean {
        val currentNumSlots = page.currentNumSlots
        val nextSlotAddress = Page.PAGE_SIZE_IN_BYTES - ((currentNumSlots + 1) * Page.SLOT_SIZE_IN_BYTES)
        return page.freeSpaceOffset + requiredSize < nextSlotAddress
    }

    private fun updateColumnIndex(key: ColumnValue, slotIndex: Int, pageId: Long) =
        indexManager.addEntry(key, pageId, slotIndex)

    private fun serializeTuple(tuple: Tuple): ByteArray {
        val schema = catalog.getTableSchema(tableName)?: throw IllegalArgumentException("Table $tableName does not exist")
        val rowSizeInBytes = catalog.getRowSizeInBytes(tableName)
        val byteBuffer = ByteBuffer.allocate(rowSizeInBytes).order(ByteOrder.LITTLE_ENDIAN)

        tuple.values.zip(schema.columnDefinitions).forEach { (tupleValue, colDef) ->
            SerializationService.serialize(tupleValue, colDef.columnType, byteBuffer)
        }

        return byteBuffer.array()
    }
}
