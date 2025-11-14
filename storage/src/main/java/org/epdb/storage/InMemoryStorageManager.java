package org.epdb.storage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.epdb.storage.dto.Page;

public class InMemoryStorageManager implements StorageManager {

    private static final int PAGE_SIZE = 4096;
    public static final long NEXT_PAGE_ID = 0L;

    private final Map<Long, byte[]> inMemoryStorage;
    private Long nextPageId;

    public InMemoryStorageManager() {
        this.inMemoryStorage = new HashMap<>();
        this.nextPageId = NEXT_PAGE_ID;
    }

    @Override
    public Page readPage(Long pageId) {
        if (!inMemoryStorage.containsKey(pageId)) {
            throw new IllegalArgumentException(String.format("Page with ID %d does not exist", pageId));
        }
        
        final var pageData = this.inMemoryStorage.get(pageId);
        System.out.printf("Reading page with ID %d%n", pageId);
        return new Page(pageId, pageData);
    }

    @Override
    public void writePage(Long pageId, byte[] data) { 
        if (data.length > PAGE_SIZE) {
            throw new IllegalArgumentException("Data size must be equal to PAGE_SIZE");
        }

        System.out.printf("Writing page with ID %d%n", pageId);
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
        var data = new byte[PAGE_SIZE];
        Arrays.fill(data, (byte) 0);
        return data;
    }
}