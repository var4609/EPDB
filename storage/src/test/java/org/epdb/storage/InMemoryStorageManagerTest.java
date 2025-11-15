package org.epdb.storage;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.*;

public class InMemoryStorageManagerTest {

    private static final int PAGE_SIZE = 4096;
    private static final int PAGE_SIZE_TOO_LARGE = PAGE_SIZE * 2;
    private static final Long MISSING_PAGE_NUMBER = 500L;

    private Long pageId;
    private InMemoryStorageManager inMemoryStorageManager;

    @Before
    public void setUp() {
        inMemoryStorageManager = new InMemoryStorageManager();
        pageId = inMemoryStorageManager.allocateNewPage();
    }

    @Test
    public void createEmptyPageDataInitializesHeader() {
        var data = inMemoryStorageManager.createEmptyPageData();

        var byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PageConstants.HEADER_SIZE_IN_BYTES, byteBuffer.getInt(PageConstants.HEADER_FREE_SPACE_OFFSET_ADDR));
        assertEquals(0, byteBuffer.getInt(PageConstants.HEADER_NUM_ROWS_ADDR));
        assertEquals(PageConstants.NO_NEXT_PAGE, byteBuffer.getInt(PageConstants.HEADER_NEXT_PAGE_ID_ADDR));
    }

    @Test
    public void allocateCreatesEmptyPage() {
        final var page = inMemoryStorageManager.readPage(pageId);
        
        assertEquals(pageId, page.pageId());
        assertEquals(PAGE_SIZE, page.data().length);
    }

    @Test
    public void allocateInitializesPageHeader() {
        final var expectedNumRows = 0;
        final var page = inMemoryStorageManager.readPage(pageId);

        var byteBuffer = ByteBuffer.wrap(page.data());
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        assertEquals(PageConstants.HEADER_SIZE_IN_BYTES, byteBuffer.getInt(PageConstants.HEADER_FREE_SPACE_OFFSET_ADDR));
        assertEquals(expectedNumRows, byteBuffer.getInt(PageConstants.HEADER_NUM_ROWS_ADDR));
        assertEquals(PageConstants.NO_NEXT_PAGE, byteBuffer.getInt(PageConstants.HEADER_NEXT_PAGE_ID_ADDR));
    }

    @Test
    public void writeAndReadPage() {
        var expectedData = new byte[]{ 7 };
        inMemoryStorageManager.writePage(pageId, expectedData);

        final var actualData = inMemoryStorageManager.readPage(pageId).data();
        assertArrayEquals(expectedData, actualData);
    }

    @Test
    public void writeWrongSizeThrows() {
        final var data = new byte[PAGE_SIZE_TOO_LARGE];
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, 
            () -> inMemoryStorageManager.writePage(pageId, data)
        );

        assertEquals("Data size must be equal to PAGE_SIZE", thrown.getMessage());
    }

    @Test
    public void readMissingThrows() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, 
            () -> inMemoryStorageManager.readPage(MISSING_PAGE_NUMBER)
        );

        assertTrue(thrown.getMessage().contains("does not exist"));
    }

    @Test
    public void allocationsIncrement() {
        final var page1 = inMemoryStorageManager.allocateNewPage();
        final var page2 = inMemoryStorageManager.allocateNewPage();
        assertEquals(page1 + 1L, (long) page2);
    }
}
