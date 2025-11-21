package org.epdb.storage.manager

import org.epdb.storage.dto.Page
import org.epdb.storage.dto.Page.Companion.HEADER_SIZE_IN_BYTES
import org.epdb.storage.dto.Page.Companion.NO_NEXT_PAGE
import org.epdb.storage.util.initializePageWithHeader
import java.nio.ByteBuffer

class InMemoryStorageManager(
    private val inMemoryStorage: MutableMap<Long, ByteArray> = mutableMapOf(),
    private var nextPageId: Long = NEXT_PAGE_ID
): StorageManager {

    companion object {
        const val PAGE_SIZE: Int = 4096
        const val NEXT_PAGE_ID: Long = 0L
    }

    override fun readPage(pageId: Long): Page {
        return if (!inMemoryStorage.contains(pageId)) {
            val newPageId = allocatePage()
            Page(newPageId, inMemoryStorage[newPageId]!!)
        } else {
            val pageData = this.inMemoryStorage[pageId]!!
            println("Reading page with ID $pageId")
            Page(pageId, pageData)
        }
    }

    override fun writePage(pageId: Long, byteArray: ByteArray) {
        return if (byteArray.size < PAGE_SIZE) {
            println("Writing page with ID $pageId")
            inMemoryStorage[pageId] = byteArray
        } else {
            throw IllegalArgumentException("Data size must be less than $PAGE_SIZE")
        }
    }

    override fun allocatePage(): Long {
        val data = createEmptyPageData()

        return this.nextPageId.also { allocatedId ->
            inMemoryStorage[allocatedId] = data
            this.nextPageId += 1
        }
    }

    override fun getAllocatedPageCount(): Int = this.inMemoryStorage.size

    fun createEmptyPageData(): ByteArray =
        ByteArray(PAGE_SIZE).also {
            ByteBuffer.wrap(it).initializePageWithHeader(
                freeSpaceOffsetValue = HEADER_SIZE_IN_BYTES,
                numRowsValue = 0,
                nextPageIdValue = NO_NEXT_PAGE
            )
        }
}