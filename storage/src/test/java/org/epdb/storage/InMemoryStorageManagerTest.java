package org.epdb.storage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.epdb.storage.dto.Page;

import static org.junit.Assert.*;

public class InMemoryStorageManagerTest {

    private static final int PAGE_SIZE = 4096;
    private StorageManager newManager;

    @Before
    public void setUp() {
        newManager = new InMemoryStorageManager();
    }

    @Test
    public void allocateCreatesEmptyPage() {
        final Long id = newManager.allcateNewPage();
        Assert.assertEquals(Long.valueOf(0L), id);

        final Page page = newManager.readPage(id);
        assertEquals(Long.valueOf(id.longValue()), Long.valueOf(page.pageId()));
        assertEquals(PAGE_SIZE, page.data().length);
    }

    @Test
    public void writeAndReadPage() {
        Long id = newManager.allcateNewPage();
        byte[] data = new byte[]{ 7 };
        newManager.writePage(id, data);

        final Page page = newManager.readPage(id);
        assertArrayEquals(data, page.data());
    }

    @Test
    public void writeWrongSizeThrows() {
        final Long id = newManager.allcateNewPage();
        byte[] data = new byte[PAGE_SIZE + 5];

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, 
            () -> {
                newManager.writePage(id, data);
            }
        );

        assertEquals("Data size must be equal to PAGE_SIZE", thrown.getMessage());
    }

    @Test
    public void readMissingThrows() {
        final Long missing = 42L;

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, 
            () -> {
                newManager.readPage(missing);
            }
        );

        assertTrue(thrown.getMessage().contains("does not exist") == true);
    }

    @Test
    public void allocationsIncrement() {
        final Long a = newManager.allcateNewPage();
        final Long b = newManager.allcateNewPage();
        Assert.assertEquals(Long.valueOf(a.longValue() + 1), Long.valueOf(b.longValue()));
    }
}
