package org.epdb.engine.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.epdb.storage.util.PageConstants.*;

public class PageUtils {

    public static void writeTupleAndSlot(
            byte[] pageData,
            byte[] serializedTuple,
            int currentNumSlots,
            int newTupleOffset
    ) {
        var newFreeSpaceOffset = newTupleOffset + serializedTuple.length;
        var byteBuffer = ByteBuffer.wrap(pageData);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        writeTuple(serializedTuple, newTupleOffset, byteBuffer);
        writeSlot(currentNumSlots, newTupleOffset, byteBuffer, serializedTuple.length);
        incrementTupleCounter(currentNumSlots, byteBuffer);
        incrementFreeSpaceOffset(newFreeSpaceOffset, byteBuffer);
    }

    public static void writeSlot(int currentNumSlots, int newTupleOffset, ByteBuffer byteBuffer, int tupleLength) {
        int newSlotAddress = PAGE_SIZE_IN_BYTES - ((currentNumSlots + 1) * SLOT_SIZE_IN_BYTES);
        byteBuffer.position(newSlotAddress);
        byteBuffer.putInt(newTupleOffset);
        byteBuffer.putInt(tupleLength);
    }

    private static void incrementTupleCounter(int currentNumSlots, ByteBuffer byteBuffer) {
        byteBuffer.putInt(HEADER_NUM_ROWS_ADDR, currentNumSlots + 1);
    }

    private static void incrementFreeSpaceOffset(int newFreeSpaceOffset, ByteBuffer byteBuffer) {
        byteBuffer.putInt(HEADER_FREE_SPACE_OFFSET_ADDR, newFreeSpaceOffset);
    }

    private static void writeTuple(byte[] serializedTuple, int newTupleOffset, ByteBuffer byteBuffer) {
        byteBuffer.position(newTupleOffset);
        byteBuffer.put(serializedTuple);
    }

    public static int readNumSlots(byte[] pageData) {
        var byteBuffer = ByteBuffer.wrap(pageData);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        return byteBuffer.getInt(HEADER_NUM_ROWS_ADDR);
    }

    public static int readFreeSpaceOffset(byte[] pageData) {
        var byteBuffer = ByteBuffer.wrap(pageData);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        return byteBuffer.getInt(HEADER_FREE_SPACE_OFFSET_ADDR);
    }
}
