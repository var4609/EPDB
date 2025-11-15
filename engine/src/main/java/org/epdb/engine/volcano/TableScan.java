package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.*;
import org.epdb.storage.dto.Page;
import org.epdb.storage.pagemanager.PageManager;

import static org.epdb.storage.pagemanager.PageConstants.TABLE_PAGE_LIMIT;

public class TableScan implements Operator {

    private final BufferManager bufferManager;
    private final Schema schema;
    private final Long tableStartPageId;
    private final PageManager pageManager;

    private int currentSlotIndex;
    private Long currentPageId;
    private Page currentPage;

    public TableScan(BufferManager bufferManager, Schema schema, Long tableStartPageId) {
        this.bufferManager = bufferManager;
        this.schema = schema;
        this.tableStartPageId = tableStartPageId;
        this.currentPage = null;
        this.currentSlotIndex = 0;
        this.pageManager = new PageManager();
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
        while(true) {
            if(currentSlotIndex >= this.pageManager.getNumSlots(currentPage)) {
                if(currentPageId > tableStartPageId + TABLE_PAGE_LIMIT - 1) {
                    return null;
                }

                currentPage = bufferManager.getPage(currentPageId);
                currentPageId += 1;
                currentSlotIndex = 0;
                continue;
            }

            var recordBytes = pageManager.getTupleFromSlotAt(currentPage, currentSlotIndex);
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
    }

    @Override
    public void close() {
         // TODO: Should unpin the pages from buffer manager in a complete implementation.
        System.out.println("Scan: Closed TableScan.");
    }
}
