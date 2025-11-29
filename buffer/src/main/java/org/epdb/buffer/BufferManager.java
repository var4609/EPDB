package org.epdb.buffer;

import org.epdb.buffer.dto.BufferFrame;
import org.epdb.storage.dto.Page;

public interface BufferManager {
    
    Page getPage(Long pageId);

    void unpinPage(Long pageId, boolean isModified);

    void flushPage(BufferFrame bufferFrame);

    Page allocateNewPage(int tableId);
}
