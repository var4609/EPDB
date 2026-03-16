package org.epdb.storage.dto

import java.nio.ByteBuffer
import java.nio.ByteOrder

class Page(val pageId: Long, val data: ByteArray) {

    companion object {
        const val PAGE_SIZE_IN_BYTES: Int = 4096
        const val HEADER_SIZE_IN_BYTES: Int = 12
        const val HEADER_FREE_SPACE_OFFSET_ADDR: Int = 0
        const val HEADER_NUM_ROWS_ADDR: Int = 4
        const val HEADER_NEXT_PAGE_ID_ADDR: Int = 8
        const val SLOT_SIZE_IN_BYTES: Int = 8
        const val SLOT_RECORD_OFFSET_SIZE_IN_BYTES: Int = 4
        const val ROW_SIZE_IN_BYTES: Int = 28
        const val NO_NEXT_PAGE: Int = -1
    }

    private val byteBuffer: ByteBuffer by lazy {
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
    }

    val currentNumSlots: Int
        get() = byteBuffer.getInt(HEADER_NUM_ROWS_ADDR)

    val freeSpaceOffset: Int
        get() = byteBuffer.getInt(HEADER_FREE_SPACE_OFFSET_ADDR)

    fun getRecordAsByteBufferBySlotId(slotIndex: Int): ByteBuffer {
        if (slotIndex >= currentNumSlots) {
            throw IndexOutOfBoundsException()
        }
        val slotLocation: Int = PAGE_SIZE_IN_BYTES - ((slotIndex + 1) * SLOT_SIZE_IN_BYTES)
        val recordLocation = byteBuffer.getInt(slotLocation)
        val recordSize = byteBuffer.getInt(slotLocation + SLOT_RECORD_OFFSET_SIZE_IN_BYTES)

        val destinationArray = ByteBuffer.allocate(recordSize).order(ByteOrder.LITTLE_ENDIAN)
        byteBuffer.get(recordLocation, destinationArray.array(), 0, recordSize)

        return destinationArray
    }

    fun hasSpaceFor(serializedTuple: ByteArray) : Boolean {
        val newTupleEndOffset = freeSpaceOffset + serializedTuple.size
        val newSlotStart = PAGE_SIZE_IN_BYTES - ((currentNumSlots + 1) * SLOT_SIZE_IN_BYTES)

        return (newTupleEndOffset < newSlotStart)
    }

    fun writeTupleAndSlot(
        serializedTuple: ByteArray
    ) {
        if (!hasSpaceFor(serializedTuple)) {
            throw IndexOutOfBoundsException("Page is full. Cannot fit ${serializedTuple.size} bytes.")
        }

        byteBuffer.put(freeSpaceOffset,serializedTuple)
        writeSlot(currentNumSlots, freeSpaceOffset, serializedTuple.size)
        byteBuffer.putInt(HEADER_NUM_ROWS_ADDR, currentNumSlots + 1)
        byteBuffer.putInt(HEADER_FREE_SPACE_OFFSET_ADDR, freeSpaceOffset + serializedTuple.size)
    }

    private fun writeSlot(currentNumSlots: Int, newTupleOffset: Int, tupleLength: Int) {
        val newSlotAddress = PAGE_SIZE_IN_BYTES - ((currentNumSlots + 1) * SLOT_SIZE_IN_BYTES)
        byteBuffer.putInt(newSlotAddress, newTupleOffset)
        val lengthAddress = newSlotAddress + SLOT_RECORD_OFFSET_SIZE_IN_BYTES
        byteBuffer.putInt(lengthAddress, tupleLength)
    }
}
