package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.IntValue;
import org.epdb.engine.dto.StringValue;
import org.epdb.engine.dto.Tuple;
import org.epdb.storage.pagemanager.PageManager;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Insert implements Operator {

    private static final int NAME_SIZE = 20;

    private final BufferManager bufferManager;
    private final Tuple tupleToInsert;
    private final Long tableStartPageId;

    private boolean isExecuted;

    public Insert(final BufferManager bufferManager, final Tuple tupleToInsert, Long tableStartPage) {
        this.bufferManager = bufferManager;
        this.tupleToInsert = tupleToInsert;
        this.isExecuted = false;
        this.tableStartPageId = tableStartPage;
    }

    @Override
    public void open() {}

    @Override
    public Tuple next() {
        if(this.isExecuted) {
            return null;
        }

        var page = bufferManager.getPage(this.tableStartPageId);
        var pageManager = new PageManager();
        var data = page.data();
        var buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        var newRowOffset = pageManager.getFreeSpaceOffsetAddr(buffer);

        if(!pageManager.isSpaceAvailable(buffer, newRowOffset, 28)) {
            //TODO: Move to next page
            return null;
        }

        buffer.position(newRowOffset);
        try {
            var id = (IntValue) tupleToInsert.getValueAtIndex(0);
            var name = (StringValue) tupleToInsert.getValueAtIndex(1);
            var age = (IntValue) tupleToInsert.getValueAtIndex(2);

            buffer.putInt(id.value());
            var nameBytes = name.value().getBytes(StandardCharsets.UTF_8);
            buffer.put(nameBytes, 0, Math.min(nameBytes.length, NAME_SIZE));
            for (int i = nameBytes.length; i < NAME_SIZE; i++) {
                buffer.put((byte) 0);
            }
            buffer.putInt(age.value());
            pageManager.addSlot(buffer, newRowOffset, 28);
            pageManager.incrementFreeSpaceOffset(buffer, newRowOffset);

            System.out.printf("Insert: Successfully serialized new row at offset %d.\n", newRowOffset);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.printf("Insert Error during serialization: %s", e.getMessage());
            this.isExecuted = true;
            return null;
        }

        this.isExecuted = true;
        return tupleToInsert;
    }

    @Override
    public void close() {}
}
