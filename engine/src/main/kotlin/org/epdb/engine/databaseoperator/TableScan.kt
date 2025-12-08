package org.epdb.engine.databaseoperator

import org.epdb.buffer.manager.BufferManager
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.dto.Schema
import org.epdb.engine.dto.Tuple
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

                if (currentPageId != tableStartPageId) {
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

            val values = buildList {
                try {
                    // TODO: Replace this hardcoded logic with schema-driven deserialization
                    add(IntValue(recordBytes.getInt())) // id

                    val nameBytes = ByteArray(20)
                    recordBytes.get(nameBytes)
                    add(StringValue(String(nameBytes).trim())) // name

                    add(IntValue(recordBytes.getInt())) // age
                } catch (e: BufferUnderflowException) {
                    System.err.println("Error reading record at page $currentPageId, slot $currentSlotIndex: Buffer Underflow.")
                    e.printStackTrace()
                    currentSlotIndex++
                    continue
                }
            }

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
