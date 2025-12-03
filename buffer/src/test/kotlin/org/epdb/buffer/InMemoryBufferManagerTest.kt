package org.epdb.buffer

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.epdb.buffer.dto.BufferFrame
import org.epdb.storage.dto.Page
import org.epdb.storage.manager.StorageManager

class InMemoryBufferManagerTest : BehaviorSpec({

    val mockStorageManager = mockk<StorageManager>()

    Given("An InMemoryBufferManager with size 3") {
        val bufferSize = 3
        val bufferManager = InMemoryBufferManager(
            bufferFrames = mutableMapOf(),
            bufferSize = bufferSize,
            storageManager = mockStorageManager
        )

        fun setupReadPage(pageId: Long, content: String = "data$pageId"): Page {
            val page = Page(pageId, content.toByteArray())
            every { mockStorageManager.readPage(pageId) } returns page
            return page
        }

        When("a non-existent page is requested (cache miss)") {
            val pageId = 1L
            val expectedPage = setupReadPage(pageId)
            val resultPage = bufferManager.getPage(pageId)

            Then("it should read the page from storage") {
                verify(exactly = 1) { mockStorageManager.readPage(pageId) }
            }

            Then("it should return the correct page") {
                resultPage shouldBe expectedPage
            }
        }

        When("an existing page is requested (cache hit)") {
            val pageId = 2L
            val initialPage = setupReadPage(pageId)
            bufferManager.getPage(pageId)

            val resultPage = bufferManager.getPage(pageId)

            Then("it should not read the page from storage again") {
                verify (exactly = 1) { mockStorageManager.readPage(pageId) }
            }

            Then("it should return the same page instance") {
                resultPage shouldBe initialPage
            }
        }

        When("the buffer is full and a new page is requested (cache eviction)") {
            val p1 = setupReadPage(1L)
            setupReadPage(2L)
            setupReadPage(3L)
            setupReadPage(4L)

            bufferManager.getPage(1L)
            bufferManager.getPage(2L)
            bufferManager.getPage(3L)

            bufferManager.unpinPage(1L, true)
            bufferManager.unpinPage(2L, false)
            bufferManager.unpinPage(3L, false)

            every { mockStorageManager.writePage(any(), any()) } just Runs

            bufferManager.getPage(4L)

            Then("it should flush the dirty victim page (1L)") {
                verify(exactly = 1) { mockStorageManager.writePage(1L, p1.data) }
            }

            Then("it should not flush other dirty pages") {
                verify(exactly = 1) { mockStorageManager.writePage(any(), any()) }
            }
        }
    }

    Given("A page is in the buffer (PageId = 10L, PinCount = 1") {
        val pageId = 10L
        val page = Page(pageId, ByteArray(4096))
        val bufferFrame = BufferFrame(page, false, 1)
        val bufferManager = InMemoryBufferManager(
            storageManager = mockStorageManager,
            bufferFrames = mutableMapOf(pageId to bufferFrame)
        )

        When("unpinPage is called without modification") {
            bufferManager.unpinPage(pageId, false)

            Then("the pinCount should decrement") {
                bufferFrame.pinCount shouldBe 0
            }

            Then("the page should remain clean") {
                bufferFrame.isDirty shouldBe false
            }
        }

        When("unpinPage is called with modification") {
            bufferFrame.pinCount = 1
            bufferFrame.isDirty = false
            bufferManager.unpinPage(pageId, true)

            Then("the pinCount should decrement") {
                bufferFrame.pinCount shouldBe 0
            }

            Then("the page should be marked dirty") {
                bufferFrame.isDirty shouldBe true
            }
        }

        When("unpin page is called when the pinCount is already 0") {
            bufferFrame.pinCount = 0

            Then("it should throw an IllegalArgumentException") {
                shouldThrow<IllegalArgumentException> {
                    bufferManager.unpinPage(pageId, false)
                }
            }
        }

    }

    Given("A page is not in the buffer") {
        val pageId = 99L
        val bufferManager = InMemoryBufferManager(
            storageManager = mockStorageManager,
            bufferFrames = mutableMapOf()
        )

        When("unpinPage is called for the missing page") {
            val result = bufferManager.unpinPage(pageId, isModified = false)

            Then("the function should return Unit (exit gracefully)") {
                result shouldBe Unit
            }
        }
    }

    Given("An InMemoryBufferManager") {
        val pageId = 50L
        val mockPage = Page(pageId, ByteArray(4096))
        every { mockStorageManager.writePage(any(), any()) } just Runs
        val bufferManager = InMemoryBufferManager(
            storageManager = mockStorageManager,
            bufferFrames = mutableMapOf(),
            bufferSize = 10
        )

        When("flushPage is called on a dirty frame") {
            val dirtyFrame = BufferFrame(mockPage,true, 1)

            bufferManager.flushPage(dirtyFrame)

            Then("it should write the page to storage") {
                verify(exactly = 1) { mockStorageManager.writePage(pageId, mockPage.data) }
            }

            Then("it should set isDirty to false") {
                dirtyFrame.isDirty shouldBe false
            }
        }

        clearMocks(mockStorageManager)
        When("flushPage is called on a clean frame") {
            val cleanFrame = BufferFrame(mockPage, false, 0)

            bufferManager.flushPage(cleanFrame)

            Then("it should not write the page to storage") {
                verify(exactly = 0) { mockStorageManager.writePage(any(), any()) }
            }

            Then("isDirty should remain false") {
                cleanFrame.isDirty shouldBe false
            }
        }
    }

    Given("The buffer manager") {
        val bufferManager = InMemoryBufferManager(
            storageManager = mockStorageManager,
            bufferFrames = mutableMapOf()
        )

        When("a new page is allocated") {
            val allocatedId = 100L
            val allocatedPage = Page(allocatedId, ByteArray(4096))

            every { mockStorageManager.allocatePage() } returns allocatedId
            every { mockStorageManager.readPage(allocatedId) } returns allocatedPage

            bufferManager.allocateNewPage(tableId = 1)

            Then("it should call allocatePage on the storage manager") {
                verify(exactly = 1) { mockStorageManager.allocatePage() }
            }
        }
    }
})