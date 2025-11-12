package org.epdb.storage;

import java.util.HashMap;
import java.util.Map;

import org.epdb.storage.dto.Page;

public class InMemoryStorageManager implements StorageManager {

    private static final int PAGE_SIZE = 4096;

    private final Map<Long, byte[]> inMemoryStorage;
    private Long nextPageId;

    public InMemoryStorageManager() {
        this.inMemoryStorage = new HashMap<>();
        this.nextPageId = 0L;
    }

    @Override
    public Page readPage(Long pageId) {
        if (!inMemoryStorage.containsKey(pageId)) {
            throw new IllegalArgumentException(String.format("Page with ID %d does not exist", pageId));
        }
        
        final var pageData = this.inMemoryStorage.get(pageId);
        System.out.println(String.format("Reading page with ID %d", pageId));
        return new Page(pageId, pageData);
    }

    @Override
    public void writePage(Long pageId, byte[] data) { 
        if (data.length > PAGE_SIZE) {
            throw new IllegalArgumentException("Data size must be equal to PAGE_SIZE");
        }

        System.out.println(String.format("Writing page with ID %d", pageId));
        inMemoryStorage.put(pageId, data);
    }

    @Override
    public Long allocateNewPage() {
        final var pageId = this.nextPageId;
        inMemoryStorage.put(pageId, createEmptyPageData());
        this.nextPageId++;
        return pageId;
    }

    private byte[] createEmptyPageData() {
        return new byte[PAGE_SIZE];
    }
}