package org.epdb.engine.databaseoperator

import org.epdb.buffer.manager.BufferManager
import org.epdb.engine.dto.Schema
import org.epdb.engine.dto.Tuple
import org.epdb.engine.serialization.RecordDecoder
import org.epdb.storage.dto.Page
import java.nio.BufferUnderflowException

class TableScan(
    private val bufferManager: BufferManager,
    private val schema: Schema,
    private val tableStartPageId: Long,
    private val maxAllocatedPageId: Long,
    private var currentSlotIndex: Int = 0,
    private var currentPageId: Long = tableStartPageId,
    private var currentPage: Page? = null
) : Operator {

    override fun open() {
        currentPage = bufferManager.getPage(currentPageId)
        currentSlotIndex = 0
        println("TableScan opened at page ID: $currentPageId")
    }

    override fun next(): Tuple? {
        while (currentPageId <= maxAllocatedPageId) {

            if (currentPage == null || currentSlotIndex >= currentPage!!.currentNumSlots) {

                if (currentPage != null) {
                    bufferManager.unpinPage(currentPageId, false)
                    currentPage = null
                }
                currentPageId += 1
                if(currentPageId > maxAllocatedPageId) {
                    break
                }

                currentPage = bufferManager.getPage(currentPageId)
                currentSlotIndex = 0
                continue
            }

            val recordBytes = currentPage!!.getRecordAsByteBufferBySlotId(currentSlotIndex)
            val values = RecordDecoder.deserialize(recordBytes, schema)

            currentSlotIndex++
            return Tuple(values)
        }

        return null
    }

    override fun close() {
        if (currentPage != null) {
            bufferManager.unpinPage(currentPageId, false)
        }

        currentPage = null
        currentSlotIndex = 0
        println("Scan: Closed TableScan.")
    }
}
