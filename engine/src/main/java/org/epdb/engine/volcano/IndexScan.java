package org.epdb.engine.volcano;

import org.epdb.buffer.BufferManager;
import org.epdb.engine.dto.*;
import org.epdb.index.IndexManager;
import org.epdb.index.dto.PagePointer;

import java.util.List;

public class IndexScan implements Operator {

    private final BufferManager bufferManager;
    private final IndexManager indexManager;
    private final Schema schema;
    private final ColumnValue searchKey;

    private int currentPagePointerIndex;
    private List<PagePointer> pagePointers;

    public IndexScan(final BufferManager bufferManager, IndexManager indexManager, final Schema schema, ColumnValue searchKey) {
        this.bufferManager = bufferManager;
        this.indexManager = indexManager;
        this.schema = schema;
        this.searchKey = searchKey;
        this.currentPagePointerIndex = 0;
    }

    @Override
    public void open() {
        this.pagePointers = indexManager.lookupEntry(this.searchKey);
        System.out.printf("IndexScan: Found %d pointers for key '%s'.\n", pagePointers.size(), searchKey);
    }

    @Override
    public Tuple next() {

        while (this.currentPagePointerIndex < pagePointers.size()) {
            var pagePointer = pagePointers.get(currentPagePointerIndex);
            var page = bufferManager.getPage(pagePointer.getPageId());

            var recordBytes = page.getRecordAsByteBufferBySlotId(pagePointer.getSlotIndex());
            ColumnValue[] values = new ColumnValue[schema.getColumnCount()];
            try {
                values[0] = new IntValue(recordBytes.getInt()); // id

                byte[] nameBytes = new byte[20];
                recordBytes.get(nameBytes);
                values[1] = new StringValue(new String(nameBytes).trim()); // name

                values[2] = new IntValue(recordBytes.getInt()); // age
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.bufferManager.unpinPage(pagePointer.getPageId(), false);
            this.currentPagePointerIndex++;
            return new Tuple(values);
        }

        return null;
    }

    @Override
    public void close() {

    }
}
