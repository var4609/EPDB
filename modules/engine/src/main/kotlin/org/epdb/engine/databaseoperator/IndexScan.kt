package org.epdb.engine.databaseoperator

import org.epdb.buffer.manager.BufferManager
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.dto.Schema
import org.epdb.engine.dto.Tuple
import org.epdb.engine.serialization.RecordDecoder
import org.epdb.index.dto.PagePointer
import org.epdb.index.manager.IndexManager
import org.epdb.org.epdb.commons.Logger

class IndexScan(
    private val bufferManager: BufferManager,
    private val indexManager: IndexManager,
    private val schema: Schema,
    private val searchKey: ColumnValue,
    private var pagePointers: List<PagePointer> = emptyList(),
    private var currentPagePointerIndex: Int = 0,
) : Operator {

    override fun open() {
        pagePointers = indexManager.lookupEntry(this.searchKey)
        Logger.info("IndexScan: Found ${pagePointers.size} pointers for key $searchKey.")
    }

    override fun next(): Tuple? {
        while (this.currentPagePointerIndex < pagePointers.size) {
            val tuple = fetchAndDecodeNextRecord(pagePointers[currentPagePointerIndex])
            currentPagePointerIndex++

            if (tuple == null) {
                continue
            }
            return tuple
        }
        return null
    }

    override fun close() {
        pagePointers = emptyList()
        currentPagePointerIndex = 0
        Logger.info("IndexScan: Closed.")
    }

    private fun fetchAndDecodeNextRecord(pagePointer: PagePointer) : Tuple? {
        val page = bufferManager.getPage(pagePointer.pageId)

        return try {
            val recordBytes = page.getRecordAsByteBufferBySlotId(pagePointer.slotIndex)
            val values : List<ColumnValue> = RecordDecoder.deserialize(recordBytes, schema)

            if (values.isEmpty()) null else Tuple(values)
        } finally {
            bufferManager.unpinPage(pagePointer.pageId, false)
        }
    }
}
