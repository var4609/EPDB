package org.epdb.buffer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.epdb.buffer.dto.BufferFrame;
import org.epdb.storage.StorageManager;
import org.epdb.storage.dto.Page;

public class InMemoryBufferManager implements BufferManager {
    
    private final StorageManager storageManager;
    private final int bufferSize;
    private final Map<Long, BufferFrame> bufferFrames;
    
    public InMemoryBufferManager(final StorageManager storageManager, final int bufferSize, final Map<Long, BufferFrame> bufferFrames) {
        this.storageManager = storageManager;
        this.bufferSize = bufferSize;
        this.bufferFrames = bufferFrames;
    }

    @Override
    public Page getPage(Long pageId) {
        if(this.bufferFrames.containsKey(pageId)) {
            return this.bufferFrames.get(pageId).page();
        } else {
            final Page page = this.storageManager.readPage(pageId);
            if(this.bufferFrames.size() >= this.bufferSize) {
                Long victimKey = Collections.min(this.bufferFrames.keySet());

                if(this.bufferFrames.get(victimKey).isDirty()) {
                    flushPage(this.bufferFrames.get(victimKey).page());
                }

                this.bufferFrames.remove(victimKey);
            }
            this.bufferFrames.put(pageId, new BufferFrame(page, false));
            return page;
        }
    }

    @Override
    public void flushPage(Page page) {
        this.storageManager.writePage(page.pageId(), page.data());
    }
}
