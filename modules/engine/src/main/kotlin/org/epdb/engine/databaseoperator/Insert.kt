package org.epdb.engine.databaseoperator

import org.epdb.buffer.manager.BufferManager
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.dto.Tuple
import org.epdb.index.manager.IndexManager
import org.epdb.storage.dto.Page
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import kotlin.math.min

class Insert(
    private val bufferManager: BufferManager,
    private val tupleToInsert: Tuple,
    private val maxAllocatedPageCount: Long,
    private val tableStartPageId: Long,
    private val indexManager: IndexManager,
    private var isExecuted: Boolean = false
) : Operator {

    companion object {
        private const val NAME_SIZE = 20
        private const val INDEXED_COLUMN_ID = 0
    }

    override fun open() {}

    override fun next(): Tuple? {
        if (this.isExecuted) return null
        this.isExecuted = true

        val serializedTuple = serializeTuple(tupleToInsert)

        for (currentPageId in tableStartPageId..maxAllocatedPageCount) {
            val page = bufferManager.getPage(currentPageId)

            try {
                if (canInsertTuple(page, serializedTuple.size)) {
                    updateColumnIndex(tupleToInsert.getValueAtIndex(INDEXED_COLUMN_ID), page.currentNumSlots, page.pageId)
                    page.writeTupleAndSlot(serializedTuple)
                    println("Inserted tuple: $tupleToInsert")
                    bufferManager.unpinPage(currentPageId, true)
                    return this.tupleToInsert
                }

                bufferManager.unpinPage(currentPageId, false)
            } catch (e: Exception) {
                bufferManager.unpinPage(currentPageId, false)
                throw e
            }
        }

        val newPage = this.bufferManager.allocateNewPage(0)
        try {
            updateColumnIndex(tupleToInsert.getValueAtIndex(INDEXED_COLUMN_ID), 0, newPage.pageId)
            newPage.writeTupleAndSlot(serializedTuple)
            println("Inserted tuple: $tupleToInsert")
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

    private fun updateColumnIndex(key: ColumnValue, slotIndex: Int, pageId: Long) {
        indexManager.addEntry(key, pageId, slotIndex)
    }

    private fun serializeTuple(tuple: Tuple): ByteArray {
        val id = tuple.getValueAtIndex(0) as? IntValue ?: throw IllegalArgumentException("Missing or invalid ID")
        val name = tuple.getValueAtIndex(1) as? StringValue ?: throw IllegalArgumentException("Missing or invalid Name")
        val age = tuple.getValueAtIndex(2) as? IntValue ?: throw IllegalArgumentException("Missing or invalid Age")

        val nameBytes = name.value.toByteArray(StandardCharsets.UTF_8)
        val byteBuffer = ByteBuffer.allocate(Page.ROW_SIZE_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN)

        byteBuffer.putInt(id.value)
        byteBuffer.put(nameBytes, 0, min(nameBytes.size, NAME_SIZE))
        for (i in nameBytes.size..<NAME_SIZE) {
            byteBuffer.put(0.toByte())
        }
        byteBuffer.putInt(age.value)

        return byteBuffer.array()
    }
}
