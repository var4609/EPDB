package org.epdb.engine.volcano;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.*;
import org.epdb.storage.dto.Page;

public class TableScan implements Operator {

    private static final Long PAGE_SIZE = 4096L;
    private static final int ROW_SIZE = 28;
    private static final int TABLE_PAGE_LIMIT = 1;

    private final BufferManager bufferManager;
    private final Schema schema;
    private final Long tableStartPageId;
    private Long currentPageId;
    private Long currentRowOffset;
    private Page currentPage;

    public TableScan(BufferManager bufferManager, Schema schema, Long tableStartPageId) {
        this.bufferManager = bufferManager;
        this.schema = schema;
        this.tableStartPageId = tableStartPageId;
        this.currentPage = null;
    }

    @Override
    public void open() {
        currentPageId = tableStartPageId;
        currentRowOffset = 0L;
        System.out.printf("TableScan opened at page ID: %d%n", currentPageId);
    }

    @Override
    public Tuple next() {
        while(true) {
            if(currentPage == null || currentRowOffset >= PAGE_SIZE) {
                if(currentPageId >= tableStartPageId + TABLE_PAGE_LIMIT) {
                    return null;
                }

                currentPage = bufferManager.getPage(currentPageId);
                currentRowOffset = 0L;
                currentPageId += 1;
                continue;
            }

            if(currentRowOffset + ROW_SIZE > PAGE_SIZE) {
                currentRowOffset = PAGE_SIZE;
                continue;
            }

            var data = currentPage.data();
            var byteBuffer = ByteBuffer.wrap(data);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.position(currentRowOffset.intValue());

            ColumnValue[] values = new ColumnValue[schema.getColumnCount()];

            try {
                values[0] = new IntValue(byteBuffer.getInt()); // id

                byte[] nameBytes = new byte[20];
                byteBuffer.get(nameBytes);
                values[1] = new StringValue(new String(nameBytes).trim()); // name

                values[2] = new IntValue(byteBuffer.getInt()); // age
            } catch (Exception e) {
                e.printStackTrace();
                currentRowOffset = PAGE_SIZE; 
                continue;
            }

            currentRowOffset += ROW_SIZE;
            return new Tuple(values);
        }
    }

    @Override
    public void close() {
         // TODO: Should unpin the pages from buffer manager in a complete implementation.
        System.out.println("Scan: Closed TableScan.");
    }
}
