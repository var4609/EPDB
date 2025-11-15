package org.epdb.storage.pagemanager;

import org.epdb.storage.dto.Page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.epdb.storage.pagemanager.PageConstants.*;

public class PageManager {

    public int getNumSlots(final Page page) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(page.data());
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer.getInt(HEADER_NUM_ROWS_ADDR);
    }

    public ByteBuffer getTupleFromSlotAt(Page page, int slotIndex) {
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
