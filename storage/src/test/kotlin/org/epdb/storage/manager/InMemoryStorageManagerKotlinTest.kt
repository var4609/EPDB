package org.epdb.storage.manager

import io.kotest.matchers.shouldBe
import org.epdb.storage.util.PageConstants.HEADER_FREE_SPACE_OFFSET_ADDR
import org.epdb.storage.util.PageConstants.HEADER_NEXT_PAGE_ID_ADDR
import org.epdb.storage.util.PageConstants.HEADER_NUM_ROWS_ADDR
import org.epdb.storage.util.PageConstants.HEADER_SIZE_IN_BYTES
import org.epdb.storage.util.PageConstants.NO_NEXT_PAGE
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.BeforeTest
import kotlin.test.Test

class InMemoryStorageManagerKotlinTest {

    companion object {
        const val PAGE_SIZE = 4096
        const val PAGE_SIZE_TOO_LARGE = PAGE_SIZE + 1
    }

    private val inMemoryStorage: MutableMap<Long, ByteArray> = mutableMapOf()
    private val nextPageId: Long = 0L
    private lateinit var inMemoryStorageManager: InMemoryStorageManager

    @BeforeTest
    fun setUp() {
        inMemoryStorageManager = InMemoryStorageManager(inMemoryStorage, nextPageId)
    }

    @Test
    fun `test createEmptyPageData() allocates page with header`() {
        val data = inMemoryStorageManager.createEmptyPageData()

        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).apply {
            this.getInt(HEADER_NEXT_PAGE_ID_ADDR) shouldBe NO_NEXT_PAGE
            this.getInt(HEADER_FREE_SPACE_OFFSET_ADDR) shouldBe HEADER_SIZE_IN_BYTES
            this.getInt(HEADER_NUM_ROWS_ADDR) shouldBe 0
        }
    }

    @Test
    fun `test readPage() creates new page if its not already present`() {
        inMemoryStorageManager.readPage(nextPageId).apply {
            this.pageId shouldBe nextPageId
            this.data.size shouldBe PAGE_SIZE
        }
    }
}