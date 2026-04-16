package org.epdb.engine.databaseoperator

import org.epdb.buffer.manager.BufferManager
import org.epdb.catalog.Catalog
import org.epdb.engine.dto.Tuple
import org.epdb.org.epdb.commons.Logger
import org.epdb.org.epdb.commons.util.SerializationService
import org.epdb.storage.dto.Page

class TableScan(
    private val bufferManager: BufferManager,
    private val catalog: Catalog,
    private val tableName: String,
    private val tableStartPageId: Long,
    private val maxAllocatedPageId: Long,
    private var currentSlotIndex: Int = 0,
    private var currentPageId: Long = tableStartPageId,
    private var currentPage: Page? = null
) : Operator {

    override fun open() {
        currentPage = bufferManager.getPage(currentPageId, tableName)
        currentSlotIndex = 0
        Logger.info("TableScan opened at page ID: $currentPageId")
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

                currentPage = bufferManager.getPage(currentPageId, tableName)
                currentSlotIndex = 0
                continue
            }

            val recordBytes = currentPage!!.getRecordAsByteBufferBySlotId(currentSlotIndex)
            val values = SerializationService.deserialize(
                byteBuffer = recordBytes,
                schema = catalog.getTableSchema(tableName) ?: throw IllegalStateException("Table $tableName does not exist.")
            )

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
        Logger.info("Scan: Closed TableScan.")
    }
}
