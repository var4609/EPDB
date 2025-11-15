package org.epdb.storage.pagemanager;

import org.epdb.storage.dto.Page;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.*;

public class PageManagerTest {

    public static final long PAGE_ID = 1L;

    private PageManager pageManager;
    private Page page;

    @Before
    public void setUp() {
        page = newEmptyPage();
        pageManager = new PageManager();
    }

    private Page newEmptyPage() {
        byte[] data = new byte[PageConstants.PAGE_SIZE_IN_BYTES];
        var byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(PageConstants.HEADER_FREE_SPACE_OFFSET_ADDR, PageConstants.HEADER_SIZE_IN_BYTES);
        byteBuffer.putInt(PageConstants.HEADER_NUM_ROWS_ADDR, 0);
        byteBuffer.putInt(PageConstants.HEADER_NEXT_PAGE_ID_ADDR, PageConstants.NO_NEXT_PAGE);
        return new Page(PAGE_ID, data);
    }

    @Test
    public void getNumSlotsOnEmptyPage() {
        assertEquals(0, pageManager.getNumSlots(page));
    }
}
