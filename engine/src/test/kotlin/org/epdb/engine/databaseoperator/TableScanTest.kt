package org.epdb.engine.databaseoperator

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.epdb.buffer.manager.BufferManager
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.dto.Schema
import org.epdb.storage.dto.Page
import java.nio.ByteBuffer
import java.nio.ByteOrder

class TableScanTest : BehaviorSpec({

    val START_PAGE_ID = 100L
    val MAX_PAGE_ID = 100L
    val MOCK_SCHEMA = Schema(emptyList())

    fun createMockPage(numSlots: Int, recordData: ByteBuffer): Page {
        val page = mockk<Page> {
            every { currentNumSlots } returns numSlots
            every { getRecordAsByteBufferBySlotId(any()) } answers {
                recordData.duplicate()
            }
        }
        return page
    }

    fun createRecord(id: Int, name: String, age: Int): ByteBuffer {
        val paddedName = name.padEnd(20, ' ').toByteArray()
        return ByteBuffer
            .allocate(28)
            .putInt(id)
            .put(paddedName)
            .putInt(age)
            .position(0)
            .order(ByteOrder.LITTLE_ENDIAN)
    }

    Given("A TableScan over a single page with 2 records") {
        val recordData = createRecord(id = 1, name = "Alice", age = 30)
        val mockPage = createMockPage(numSlots = 2, recordData = recordData)
        val bufferManager : BufferManager = mockk {
            every { getPage(START_PAGE_ID) } returns mockPage
            every { getPage(MAX_PAGE_ID) } returns mockPage
            every { unpinPage(any(), any()) } just Runs
        }

        val scan = TableScan(
            bufferManager = bufferManager,
            schema =  MOCK_SCHEMA,
            tableStartPageId =  START_PAGE_ID,
            maxAllocatedPageId =  MAX_PAGE_ID
        )

        When("The scan is opened") {
            scan.open()

            Then("The start page should be pinned") {
                verify (exactly = 1) {
                    bufferManager.getPage(START_PAGE_ID)
                }
            }
        }

        When("next() is called once") {
            val tuple = scan.next()

            Then("It should return the first tuple (Alice)") {
                tuple shouldNotBe null
                tuple!!.values.size shouldBe 3
                tuple.values[0] shouldBe IntValue(1)
            }
        }

        When("next() is called a second time") {
            val tuple = scan.next()

            Then("It should return the second tuple") {
                tuple shouldNotBe null
            }
        }

        When("next() is called a third time (past the end)") {
            val tuple = scan.next()

            Then("It should return null") {
                tuple shouldBe null
            }

            Then("The page should be unpinned") {
                verify (exactly = 1) {
                    bufferManager.unpinPage(any(), any())
                }
            }
        }

        When("close() is called") {
            scan.close()

            Then("The current page should be unpinned") {
                verify (exactly = 1) {
                    bufferManager.unpinPage(START_PAGE_ID, false)
                }
            }
        }
    }
})
