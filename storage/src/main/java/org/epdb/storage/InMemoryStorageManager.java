package org.epdb.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import org.epdb.storage.dto.Page;

import static org.epdb.storage.pagemanager.PageConstants.*;

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
            Long newPageId = allocateNewPage();
            return new Page(newPageId, inMemoryStorage.get(newPageId));
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
        final var data = createEmptyPageData();

        inMemoryStorage.put(pageId, data);
        this.nextPageId++;
        return pageId;
    }

    /**
     * Standing utility for upstream classes to get page size dynamically.
     * @return current number of allocated pages.
     */
    public int getAllocatedPageCount() {
        return this.inMemoryStorage.size();
    }

    /**
     * Create a fresh page byte array with the appropriate header initialized.
     */
    byte[] createEmptyPageData() {
        final var data = new byte[PAGE_SIZE];
        var byteBuffer = ByteBuffer.wrap(data);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(HEADER_FREE_SPACE_OFFSET_ADDR, HEADER_SIZE_IN_BYTES);
        byteBuffer.putInt(HEADER_NUM_ROWS_ADDR, 0);
        byteBuffer.putInt(HEADER_NEXT_PAGE_ID_ADDR, NO_NEXT_PAGE);
        return data;
    }
}