package org.epdb.storage;

import org.junit.Before;
import org.junit.Test;
import org.epdb.storage.dto.Page;

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
        final Long pageId = storageManager.allcateNewPage();
        final Page page = storageManager.readPage(pageId);
        
        assertEquals(pageId, page.pageId());
        assertEquals(PAGE_SIZE, page.data().length);
    }

    @Test
    public void writeAndReadPage() {
        Long pageId = storageManager.allcateNewPage();
        byte[] expectedData = new byte[]{ 7 };
        storageManager.writePage(pageId, expectedData);

        final byte[] actualData = storageManager.readPage(pageId).data();
        assertArrayEquals(expectedData, actualData);
    }

    @Test
    public void writeWrongSizeThrows() {
        final Long pageId = storageManager.allcateNewPage();
        byte[] data = new byte[PAGE_SIZE_TOO_LARGE];

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

        assertTrue(thrown.getMessage().contains("does not exist") == true);
    }

    @Test
    public void allocationsIncrement() {
        final Long page1 = storageManager.allcateNewPage();
        final Long page2 = storageManager.allcateNewPage();
        assertTrue(page1 + 1L == page2);
    }
}
