package org.epdb.storage.dto

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer
import java.nio.ByteOrder

class PageTest : FunSpec({

    val PAGE_SIZE = Page.PAGE_SIZE_IN_BYTES
    val HEADER_NUM_ROWS_ADDR = Page.HEADER_NUM_ROWS_ADDR
    val HEADER_FREE_SPACE_OFFSET_ADDR = Page.HEADER_FREE_SPACE_OFFSET_ADDR
    val HEADER_SIZE = Page.HEADER_SIZE_IN_BYTES
    val SLOT_SIZE = Page.SLOT_SIZE_IN_BYTES

    fun createEmptyPage(): Pair<Page, ByteBuffer> {
        val data = ByteArray(PAGE_SIZE)
        val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)

        buffer.putInt(HEADER_NUM_ROWS_ADDR, 0)
        buffer.putInt(HEADER_FREE_SPACE_OFFSET_ADDR, HEADER_SIZE)

        return Pair(Page(1L, data), buffer)
    }

    context("Header Read Properties") {
        test("should correctly read zero slots from an empty page") {
            val (page, _) = createEmptyPage()
            page.currentNumSlots shouldBe 0
        }

        test("should correctly read initial free space offset") {
            val (page, _) = createEmptyPage()
            page.freeSpaceOffset shouldBe HEADER_SIZE
        }
    }

    context("Write Tuple And Slot") {
        test("should successfully write the first tuple and update header") {
            val (page, buffer) = createEmptyPage()
            val tupleData = byteArrayOf(10, 20, 30, 40)

            page.writeTupleAndSlot(tupleData)

            page.currentNumSlots shouldBe 1
            page.freeSpaceOffset shouldBe HEADER_SIZE + 4

            val writtenData = ByteArray(4)
            buffer.position(HEADER_SIZE)
            buffer.get(writtenData)
            writtenData shouldBe tupleData

            val expectedSlotAddress = PAGE_SIZE - SLOT_SIZE
            buffer.position(expectedSlotAddress)
            buffer.getInt() shouldBe HEADER_SIZE
            buffer.getInt() shouldBe 4
        }

        test("should write a second tuple correctly, advancing headers and slots") {
            val (page, buffer) = createEmptyPage()
            val tuple1 = byteArrayOf(1)
            val tuple2 = byteArrayOf(2, 3)

            page.writeTupleAndSlot(tuple1)
            val offset2 = HEADER_SIZE + tuple1.size

            page.writeTupleAndSlot(tuple2)

            page.currentNumSlots shouldBe 2
            page.freeSpaceOffset shouldBe offset2 + tuple2.size

            val expectedSlotAddress = PAGE_SIZE - (2 * SLOT_SIZE)
            buffer.position(expectedSlotAddress)
            buffer.getInt() shouldBe offset2
            buffer.getInt() shouldBe 2
        }

        test("should throw IndexOutOfBoundsException when page runs out of space") {
            val (page, _) = createEmptyPage()
            val minimalTuple = ByteArray(1)
            val totalSpace = PAGE_SIZE - HEADER_SIZE
            val maxTuplesThatFit = totalSpace / (1 + SLOT_SIZE)

            repeat(maxTuplesThatFit) {
                page.writeTupleAndSlot(minimalTuple)
            }

            page.currentNumSlots shouldBe maxTuplesThatFit

            shouldThrow<IndexOutOfBoundsException> {
                page.writeTupleAndSlot(minimalTuple)
            }
        }
    }

    context("getTuple") {
        test("should retrieve the first tuple correctly after writing") {
            val (page, _) = createEmptyPage()
            val tupleData = byteArrayOf(11, 22, 33)

            page.writeTupleAndSlot(tupleData)

            val retrievedBuffer = page.getTuple(slotIndex = 0)

            retrievedBuffer.capacity() shouldBe 3
            retrievedBuffer.array() shouldBe tupleData
        }

        test("should retrieve the second tuple correctly after multiple writes") {
            val (page, _) = createEmptyPage()
            val tuple1 = byteArrayOf(50)
            val tuple2 = byteArrayOf(60, 70, 80, 90)

            page.writeTupleAndSlot(tuple1)
            page.writeTupleAndSlot(tuple2)

            val retrievedBuffer = page.getTuple(slotIndex = 1)

            retrievedBuffer.capacity() shouldBe 4
            retrievedBuffer.array() shouldBe tuple2
        }

        test("should throw exception when slotIndex is negative") {
            val (page, _) = createEmptyPage()
            page.writeTupleAndSlot(byteArrayOf(1))

            shouldThrow<IndexOutOfBoundsException> {
                page.getTuple(slotIndex = -1)
            }
        }

        test("should throw exception when slotIndex is equal to currentNumSlots (out of bounds)") {
            val (page, _) = createEmptyPage()
            page.writeTupleAndSlot(byteArrayOf(1))

            shouldThrow<IndexOutOfBoundsException> {
                page.getTuple(slotIndex = 1)
            }
        }
    }
})