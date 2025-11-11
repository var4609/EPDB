package org.epdb.buffer;

import org.epdb.storage.dto.Page;

public interface BufferManager {
    
    Page getPage(Long pageId);

    void flushPage(Page page);
}
