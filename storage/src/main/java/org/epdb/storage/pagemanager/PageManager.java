package org.epdb.storage.pagemanager;

import org.epdb.storage.dto.Page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PageManager {

    private static final int PAGE_SIZE_IN_BYTES = 4096;
    private static final int HEADER_SIZE_IN_BYTES = 8;
    private static final int HEADER_FREE_SPACE_OFFSET_ADDR = 0;
    private static final int HEADER_NUM_ROWS_ADDR = 4;
    private static final int SLOT_SIZE_IN_BYTES = 8;
    private static final int SLOT_RECORD_OFFSET_SIZE_IN_BYTES = 4;
    private static final int ROW_SIZE_IN_BYTES = 28;

    public int getFreeSpaceOffsetAddr(final ByteBuffer byteBuffer) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        if (isEmpty(byteBuffer)) {
            return HEADER_FREE_SPACE_OFFSET_ADDR + HEADER_SIZE_IN_BYTES;
        }
        return byteBuffer.getInt(HEADER_FREE_SPACE_OFFSET_ADDR);
    }

    public boolean isEmpty(ByteBuffer byteBuffer) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer.getInt(HEADER_NUM_ROWS_ADDR) == 0;
    }

    public int getNumSlots(final Page page) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(page.data());
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer.getInt(HEADER_NUM_ROWS_ADDR);
    }

    public int getSlotOffset(final ByteBuffer byteBuffer) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        var numRows = byteBuffer.getInt(HEADER_NUM_ROWS_ADDR);
        return PAGE_SIZE_IN_BYTES - (SLOT_SIZE_IN_BYTES * (numRows + 1));
    }

    public void incrementFreeSpaceOffset(final ByteBuffer byteBuffer, final int oldOffset) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int newVal = oldOffset + ROW_SIZE_IN_BYTES;
        ByteBuffer temp = ByteBuffer.allocate(4);
        temp.putInt(HEADER_FREE_SPACE_OFFSET_ADDR, newVal);
        byteBuffer.putInt(HEADER_FREE_SPACE_OFFSET_ADDR, newVal);
        incrementRowCounter(byteBuffer);
    }

    public void incrementRowCounter(ByteBuffer byteBuffer) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int oldCount = byteBuffer.getInt(HEADER_NUM_ROWS_ADDR);
        byteBuffer.putInt(HEADER_NUM_ROWS_ADDR, oldCount + 1);
    }

    public void addSlot(final ByteBuffer byteBuffer, final int recordOffset, final int recordSize) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        final int newSlotRecordOffset = getSlotOffset(byteBuffer);
        final int newSlotRecordLengthOffset = newSlotRecordOffset + SLOT_RECORD_OFFSET_SIZE_IN_BYTES;
        try {
            byteBuffer.putInt(newSlotRecordOffset, recordOffset);
            byteBuffer.putInt(newSlotRecordLengthOffset, recordSize);
        } catch (Exception e) {
            System.out.printf("SlotRecordOffset: %d, SlotRecordLengthOffset: %d \n", newSlotRecordOffset, newSlotRecordLengthOffset);
        }
    }

    public boolean isSpaceAvailable(final ByteBuffer byteBuffer, int rowOffset, int newRowSize) {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return rowOffset + newRowSize < getSlotOffset(byteBuffer);
    }

    public ByteBuffer getRecordFromSlotAt(Page page, int slotIndex) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(page.data()).duplicate();
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        var slotLocation = PAGE_SIZE_IN_BYTES - ((slotIndex + 1) * SLOT_SIZE_IN_BYTES);
        var recordLocation = byteBuffer.getInt(slotLocation);
        var recordSize = byteBuffer.getInt(slotLocation + SLOT_RECORD_OFFSET_SIZE_IN_BYTES);

        ByteBuffer destinationArray = ByteBuffer.allocate(recordSize).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.get(recordLocation, destinationArray.array(), 0, recordSize);

        return destinationArray;
    }
}
