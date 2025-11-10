package org.epdb.storage;

import org.epdb.storage.dto.Page;

public interface StorageManager {
    
    Page readPage(Long pageId);

    void writePage(Long pageId, byte[] data);

    Long allcateNewPage();
}