package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.columntypes.ColumnValue;
import org.epdb.engine.columntypes.IntValue;
import org.epdb.engine.columntypes.StringValue;
import org.epdb.engine.dto.Tuple;
import org.epdb.index.IndexManager;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static org.epdb.storage.dto.Page.*;

public class Insert implements Operator {

    private static final int NAME_SIZE = 20;
    private static final int INDEXED_COLUMN_ID = 0;

    private final IndexManager indexManager;
    private final BufferManager bufferManager;
    private final Tuple tupleToInsert;
    private final Long tableStartPageId;
    private final int maxAllocatedPageCount;

    private boolean isExecuted;

    public Insert(final BufferManager bufferManager, final Tuple tupleToInsert, final int maxAllocatedPageCount, Long tableStartPageId, final IndexManager indexManager) {
        this.indexManager = indexManager;
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

            var currentFreeSpaceOffset = page.getFreeSpaceOffset();
            var currentNumSlots = page.getCurrentNumSlots();
            var nextSlotAddress = PAGE_SIZE_IN_BYTES - ((currentNumSlots + 1) * SLOT_SIZE_IN_BYTES);

            if (currentFreeSpaceOffset + serializedTuple.length < nextSlotAddress) {
                updateColumnIndex(tupleToInsert.getValueAtIndex(INDEXED_COLUMN_ID), currentNumSlots, page.getPageId());
                page.writeTupleAndSlot(serializedTuple);
                System.out.println("Inserted tuple: " + tupleToInsert);
                bufferManager.unpinPage(currentPageId, true);
                return this.tupleToInsert;
            } else {
                bufferManager.unpinPage(currentPageId, false);
            }
        }

        var page = this.bufferManager.allocateNewPage(0);
        page.writeTupleAndSlot(serializedTuple);
        updateColumnIndex(tupleToInsert.getValueAtIndex(INDEXED_COLUMN_ID), 0, page.getPageId());;
        System.out.println("Inserted tuple: " + tupleToInsert);
        return this.tupleToInsert;
    }

    @Override
    public void close() {}

    private void updateColumnIndex(ColumnValue key, int slotIndex, Long pageId) {
        indexManager.addEntry(key, pageId, slotIndex);
    }

    private byte[] serializeTuple(Tuple tuple) {
        var id = (IntValue) tuple.getValueAtIndex(0);
        var name = (StringValue) tuple.getValueAtIndex(1);
        var nameBytes = name.getValue().getBytes(StandardCharsets.UTF_8);
        var age = (IntValue) tuple.getValueAtIndex(2);
        var byteBuffer = ByteBuffer.allocate(ROW_SIZE_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN);

        byteBuffer.putInt(id.getValue());
        byteBuffer.put(nameBytes, 0, Math.min(nameBytes.length, NAME_SIZE));
        for (int i = nameBytes.length; i < NAME_SIZE; i++) {
            byteBuffer.put((byte) 0);
        }
        byteBuffer.putInt(age.getValue());

        return byteBuffer.array();
    }
}
