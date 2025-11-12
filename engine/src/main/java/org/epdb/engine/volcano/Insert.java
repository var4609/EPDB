package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Insert implements Operator {

    private static final int ROW_SIZE = 28;
    private static final int NAME_SIZE = 20;

    private final BufferManager bufferManager;
    private final Tuple tupleToInsert;
    private final Long tableStartPageId;

    private int currentRowCount;
    private boolean isExecuted;

    public Insert(final BufferManager bufferManager, final Tuple tupleToInsert, Long tableStartPage) {
        this.bufferManager = bufferManager;
        this.tupleToInsert = tupleToInsert;
        this.isExecuted = false;
        this.tableStartPageId = tableStartPage;
        this.currentRowCount = 0;
    }

    @Override
    public void open() {}

    @Override
    public Tuple next() {
        if(this.isExecuted) {
            return null;
        }

        System.out.printf("Insert: Starting execution for tuple: %s", tupleToInsert);
        var page = bufferManager.getPage(this.tableStartPageId);
        var data = page.data();
        var insertOffset = this.currentRowCount * ROW_SIZE;
        var buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(insertOffset);

        try {
            var id = (Integer) tupleToInsert.getValueAtIndex(0);
            var name = (String) tupleToInsert.getValueAtIndex(1);
            var age = (Integer) tupleToInsert.getValueAtIndex(2);
            buffer.putInt(id);
            var nameBytes = name.getBytes(StandardCharsets.UTF_8);
            buffer.put(nameBytes, 0, Math.min(nameBytes.length, NAME_SIZE));
            for (int i = nameBytes.length; i < NAME_SIZE; i++) {
                buffer.put((byte) 0);
            }
            buffer.putInt(age);
            this.currentRowCount++;
            System.out.printf("Insert: Successfully serialized new row at offset %d.\n", insertOffset);

        } catch (Exception e) {
            System.err.printf("Insert Error during serialization: %s", e.getMessage());
            this.isExecuted = true;
            return null;
        }

        this.isExecuted = true;
        return tupleToInsert;
    }

    @Override
    public void close() {
        System.out.println("Insert: Closed operator.");
    }
}
