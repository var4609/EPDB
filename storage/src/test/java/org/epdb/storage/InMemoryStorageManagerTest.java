package org.epdb.storage;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class InMemoryStorageManagerTest {

    private static final int PAGE_SIZE = 4096;
    private static final int PAGE_SIZE_TOO_LARGE = PAGE_SIZE * 2;
    private static final Long MISSING_PAGE_NUMBER = 500L;

    private StorageManager storageManager;

    @Before
    public void setUp() {
        storageManager = new InMemoryStorageManager();
    }

    @Test
    public void allocateCreatesEmptyPage() {
        final var pageId = storageManager.allocateNewPage();
        final var page = storageManager.readPage(pageId);
        
        assertEquals(pageId, page.pageId());
        assertEquals(PAGE_SIZE, page.data().length);
    }

    @Test
    public void writeAndReadPage() {
        var pageId = storageManager.allocateNewPage();
        var expectedData = new byte[]{ 7 };
        storageManager.writePage(pageId, expectedData);

        final var actualData = storageManager.readPage(pageId).data();
        assertArrayEquals(expectedData, actualData);
    }

    @Test
    public void writeWrongSizeThrows() {
        final var pageId = storageManager.allocateNewPage();
        final var data = new byte[PAGE_SIZE_TOO_LARGE];

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, 
            () -> {
                storageManager.writePage(pageId, data);
            }
        );

        assertEquals("Data size must be equal to PAGE_SIZE", thrown.getMessage());
    }

    @Test
    public void readMissingThrows() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, 
            () -> {
                storageManager.readPage(MISSING_PAGE_NUMBER);
            }
        );

        assertTrue(thrown.getMessage().contains("does not exist"));
    }

    @Test
    public void allocationsIncrement() {
        final var page1 = storageManager.allocateNewPage();
        final var page2 = storageManager.allocateNewPage();
        assertEquals(page1 + 1L, (long) page2);
    }
}
