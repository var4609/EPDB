package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.*;
import org.epdb.storage.dto.Page;

public class TableScan implements Operator {

    private final BufferManager bufferManager;
    private final Schema schema;
    private final Long tableStartPageId;
    private final int maxAllocatedPageId;

    private int currentSlotIndex;
    private Long currentPageId;
    private Page currentPage;

    public TableScan(BufferManager bufferManager, Schema schema, Long tableStartPageId, int maxAllocatedPageId) {
        this.bufferManager = bufferManager;
        this.schema = schema;
        this.tableStartPageId = tableStartPageId;
        this.currentPage = null;
        this.currentSlotIndex = 0;
        this.maxAllocatedPageId = maxAllocatedPageId;
    }

    @Override
    public void open() {
        currentPageId = tableStartPageId;
        currentSlotIndex = 0;
        currentPage = bufferManager.getPage(currentPageId);
        System.out.printf("TableScan opened at page ID: %d%n", currentPageId);
    }

    @Override
    public Tuple next() {
        while(currentPageId <= this.maxAllocatedPageId) {
            if(currentSlotIndex >= currentPage.getCurrentNumSlots()) {
                currentPageId += 1;
                currentPage = bufferManager.getPage(currentPageId);
                currentSlotIndex = 0;
                continue;
            }

            var recordBytes = currentPage.getTuple(currentSlotIndex);
            ColumnValue[] values = new ColumnValue[schema.getColumnCount()];
            try {
                values[0] = new IntValue(recordBytes.getInt()); // id

                byte[] nameBytes = new byte[20];
                recordBytes.get(nameBytes);
                values[1] = new StringValue(new String(nameBytes).trim()); // name

                values[2] = new IntValue(recordBytes.getInt()); // age
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            this.currentSlotIndex++;
            return new Tuple(values);
        }

        return null;
    }

    @Override
    public void close() {
         // TODO: Should unpin the pages from buffer manager in a complete implementation.
        System.out.println("Scan: Closed TableScan.");
    }
}
