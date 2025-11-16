package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.IntValue;
import org.epdb.engine.dto.StringValue;
import org.epdb.engine.dto.Tuple;
import org.epdb.engine.util.PageUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static org.epdb.storage.pagemanager.PageConstants.*;

public class Insert implements Operator {

    private static final int NAME_SIZE = 20;

    private final BufferManager bufferManager;
    private final Tuple tupleToInsert;
    private final Long tableStartPageId;
    private final int maxAllocatedPageCount;

    private boolean isExecuted;

    public Insert(final BufferManager bufferManager, final Tuple tupleToInsert, final int maxAllocatedPageCount, Long tableStartPageId) {
        this.bufferManager = bufferManager;
        this.tupleToInsert = tupleToInsert;
        this.isExecuted = false;
        this.tableStartPageId = tableStartPageId;
        this.maxAllocatedPageCount = maxAllocatedPageCount;
    }

    @Override
    public void open() {}

    @Override
    public Tuple next() {
        if(this.isExecuted) {
            return null;
        }

        this.isExecuted = true;
        var serializedTuple = serializeTuple(this.tupleToInsert);

        for(var currentPageId = this.tableStartPageId; currentPageId <= this.maxAllocatedPageCount; currentPageId++) {
            var page = bufferManager.getPage(currentPageId);
            var pageData = page.data();

            var currentFreeSpaceOffset = PageUtils.readFreeSpaceOffset(pageData);
            var currentNumSlots = PageUtils.readNumSlots(pageData);
            var nextSlotAddress = PAGE_SIZE_IN_BYTES - ((currentNumSlots + 1) * SLOT_SIZE_IN_BYTES);

            if (currentFreeSpaceOffset + serializedTuple.length <= nextSlotAddress) {
                PageUtils.writeTupleAndSlot(pageData, serializedTuple, currentNumSlots, currentFreeSpaceOffset);
                System.out.println(tupleToInsert);
                // bufferManager.unpinPage(currentPageId, true);
                return this.tupleToInsert;
            } else {
                // bufferManager.unpinPage(currentPageId, false);
            }
        }

        var page = this.bufferManager.allocateNewPage(0);
        PageUtils.writeTupleAndSlot(page.data(), serializedTuple, 0, HEADER_SIZE_IN_BYTES);
        System.out.println(tupleToInsert);
        return this.tupleToInsert;
    }

    @Override
    public void close() {}

    private byte[] serializeTuple(Tuple tuple) {
        var id = (IntValue) tuple.getValueAtIndex(0);
        var name = (StringValue) tuple.getValueAtIndex(1);
        var nameBytes = name.value().getBytes(StandardCharsets.UTF_8);
        var age = (IntValue) tuple.getValueAtIndex(2);
        var byteBuffer = ByteBuffer.allocate(ROW_SIZE_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN);

        byteBuffer.putInt(id.value());
        byteBuffer.put(nameBytes, 0, Math.min(nameBytes.length, NAME_SIZE));
        for (int i = nameBytes.length; i < NAME_SIZE; i++) {
            byteBuffer.put((byte) 0);
        }
        byteBuffer.putInt(age.value());

        return byteBuffer.array();
    }
}
