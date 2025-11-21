package org.epdb.buffer;

import java.util.Collections;
import java.util.Map;

import org.epdb.buffer.dto.BufferFrame;
import org.epdb.storage.dto.Page;
import org.epdb.storage.manager.StorageManagerKotlin;

public record InMemoryBufferManager(
        StorageManagerKotlin storageManager,
        int bufferSize,
        Map<Long, BufferFrame> bufferFrames
) implements BufferManager {

    @Override
    public Page getPage(Long pageId) {
        if (this.bufferFrames.containsKey(pageId)) {
            return this.bufferFrames.get(pageId).page();
        } else {
            final var page = this.storageManager.readPage(pageId);
            if (this.bufferFrames.size() >= this.bufferSize) {
                removeUnusedPage();
            }
            this.bufferFrames.put(pageId, new BufferFrame(page, false));
            return page;
        }
    }

    @Override
    public void flushPage(Page page) {
        this.storageManager.writePage(page.pageId(), page.data());
    }

    @Override
    public Page allocateNewPage(int tableId) {
        var newPageId = this.storageManager.allocatePage();
        return getPage(newPageId);
    }

    private void removeUnusedPage() {
        var victimKey = Collections.min(this.bufferFrames.keySet());
        if (this.bufferFrames.get(victimKey).isDirty()) {
            flushPage(this.bufferFrames.get(victimKey).page());
        }

        this.bufferFrames.remove(victimKey);
    }
}
