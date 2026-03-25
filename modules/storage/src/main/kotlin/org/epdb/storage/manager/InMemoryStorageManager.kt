package org.epdb.storage.manager

import org.epdb.catalog.Catalog
import org.epdb.org.epdb.commons.Logger
import org.epdb.storage.dto.Page
import org.epdb.storage.dto.Page.Companion.HEADER_SIZE_IN_BYTES
import org.epdb.storage.dto.Page.Companion.NO_NEXT_PAGE
import org.epdb.storage.util.GlobalPageIdAllocator
import org.epdb.storage.util.initializePageWithHeader
import java.nio.ByteBuffer

internal class InMemoryStorageManager(
    private val storageProvider: MutableMap<Long, ByteArray> = mutableMapOf(),
    private val catalog: Catalog
): StorageManager {

    companion object {
        const val PAGE_SIZE: Int = 4096
    }

    override fun readPage(pageId: Long, tableName: String): Page {
        return if (!storageProvider.contains(pageId)) {
            val newPageId = allocatePage(tableName)
            Page(newPageId, storageProvider[newPageId]!!)
        } else {
            val pageData = this.storageProvider[pageId]!!
            Logger.info("Reading page with ID $pageId")
            Page(pageId, pageData)
        }
    }

    override fun writePage(pageId: Long, byteArray: ByteArray) {
        return if (byteArray.size < PAGE_SIZE) {
            Logger.info("Writing page with ID $pageId")
            storageProvider[pageId] = byteArray
        } else {
            throw IllegalArgumentException("Data size must be less than $PAGE_SIZE")
        }
    }

    override fun allocatePage(tableName: String): Long {
        val data = createEmptyPageData()
        return GlobalPageIdAllocator.nextPageId.also { allocatedId ->
            storageProvider[allocatedId] = data
            catalog.addPageIdToTable(tableName, allocatedId)
        }
    }

    override fun getAllocatedPageCount(): Int = this.storageProvider.size

    fun createEmptyPageData(): ByteArray =
        ByteArray(PAGE_SIZE).also {
            ByteBuffer.wrap(it).initializePageWithHeader(
                freeSpaceOffsetValue = HEADER_SIZE_IN_BYTES,
                numRowsValue = 0,
                nextPageIdValue = NO_NEXT_PAGE
            )
        }
}