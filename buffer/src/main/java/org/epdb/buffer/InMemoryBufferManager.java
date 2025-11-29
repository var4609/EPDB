package org.epdb.buffer;

import java.util.Collections;
import java.util.Map;

import org.epdb.buffer.dto.BufferFrame;
import org.epdb.storage.dto.Page;
import org.epdb.storage.manager.StorageManager;

public record InMemoryBufferManager(
        StorageManager storageManager,
        int bufferSize,
        Map<Long, BufferFrame> bufferFrames
) implements BufferManager {

    @Override
    public Page getPage(Long pageId) {
        if (this.bufferFrames.containsKey(pageId)) {
            var frame = this.bufferFrames.get(pageId);

            frame.pinCount++;
            return frame.page;
        }

        if (this.bufferFrames.size() >= this.bufferSize) {
            evictPage();
        }

        final var newPage = this.storageManager.readPage(pageId);
        final var frame = new BufferFrame(newPage);
        frame.pinCount++;
        this.bufferFrames.put(pageId, frame);

        return newPage;
    }

    @Override
    public void unpinPage(Long pageId, boolean isModified) {
        var bufferFrame = this.bufferFrames.get(pageId);

        if (bufferFrame == null) {
            throw new IllegalArgumentException("Page does not exist.");
        }

        if (bufferFrame.pinCount <= 0) {
            System.err.println("Illegal unpin action on page.");
        }

        bufferFrame.pinCount--;

        if (isModified) {
            bufferFrame.isDirty = true;
        }
    }

    @Override
    public void flushPage(BufferFrame frame) {
        if(frame.isDirty) {
            var page = frame.page;
            this.storageManager.writePage(page.getPageId(), page.getData());
            frame.isDirty = false;
        }
    }

    @Override
    public Page allocateNewPage(int tableId) {
        var newPageId = this.storageManager.allocatePage();
        return getPage(newPageId);
    }

    private void evictPage() {
        var victimKey = Collections.min(this.bufferFrames.keySet());
        flushPage(this.bufferFrames.get(victimKey));
        this.bufferFrames.remove(victimKey);
    }
}
