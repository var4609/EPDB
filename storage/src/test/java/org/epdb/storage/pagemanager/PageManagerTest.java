package org.epdb.storage.pagemanager;

import org.epdb.storage.dto.Page;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.epdb.storage.PageConstants.PAGE_SIZE_IN_BYTES;
import static org.epdb.storage.PageConstants.ROW_SIZE_IN_BYTES;
import static org.epdb.storage.PageConstants.SLOT_SIZE_IN_BYTES;
import static org.junit.Assert.*;

public class PageManagerTest {

    public static final long PAGE_ID = 1L;

    private PageManager pageManager;
    private Page page;
    private ByteBuffer byteBuffer;
    
    @Before
    public void setUp() {
        page = newEmptyPage();
        byteBuffer = ByteBuffer.wrap(page.data());
        pageManager = new PageManager();
    }

    private Page newEmptyPage() {
        return TestPageFactory.createEmptyPage(PAGE_ID);
    }

    @Test
    public void emptyPageIsEmpty() {
        assertTrue(pageManager.isEmpty(byteBuffer));
    }

    @Test
    public void getFreeSpaceOffsetOnEmptyPage() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        assertEquals(8, free);
    }

    @Test
    public void getNumSlotsOnEmptyPage() {
        assertEquals(0, pageManager.getNumSlots(page));
    }

    @Test
    public void incrementFreeSpaceOffsetUpdatesFreeOffset() {
        var old = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        pageManager.incrementFreeSpaceOffset(byteBuffer, old);
        assertEquals(old + ROW_SIZE_IN_BYTES, byteBuffer.getInt(0));
    }

    @Test
    public void incrementFreeSpaceOffsetIncrementsRowCount() {
        var old = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        pageManager.incrementFreeSpaceOffset(byteBuffer, old);
        assertEquals(1, byteBuffer.getInt(4));
    }

    @Test
    public void addSlotWritesSlotHeaderOffsetAndLength() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var rec = new byte[ROW_SIZE_IN_BYTES];
        byteBuffer.put(free, rec, 0, rec.length);

        pageManager.addSlot(byteBuffer, free, rec.length);

        int slot0 = PAGE_SIZE_IN_BYTES - SLOT_SIZE_IN_BYTES;
        assertEquals(free, byteBuffer.getInt(slot0));
        assertEquals(rec.length, byteBuffer.getInt(slot0 + 4));
    }

    @Test
    public void getRecordFromSlotAtReturnsTuple() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var rec = new byte[ROW_SIZE_IN_BYTES];
        for (var i = 0; i < rec.length; i++) {
            rec[i] = (byte) (i + 5);
        }
        byteBuffer.put(free, rec, 0, rec.length);
        pageManager.addSlot(byteBuffer, free, rec.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free);

        var got = pageManager.getTupleFromSlotAt(page, 0);
        assertNotNull(got);
        var out = new byte[rec.length];
        got.get(out);
        assertArrayEquals(rec, out);
    }

    @Test
    public void isSpaceAvailableTrueForSmall() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        assertTrue(pageManager.isSpaceAvailable(byteBuffer, free, 16));
    }

    @Test
    public void isSpaceAvailableFalseForExactAndOver() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var slotOff = pageManager.getSlotOffset(byteBuffer);
        assertFalse(pageManager.isSpaceAvailable(byteBuffer, free, slotOff - free));
        assertFalse(pageManager.isSpaceAvailable(byteBuffer, free, slotOff - free + 1));
    }

    @Test
    public void getSlotOffsetChangesWithRows() {
        var slot1 = pageManager.getSlotOffset(byteBuffer);

        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        pageManager.addSlot(byteBuffer, free, ROW_SIZE_IN_BYTES);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free);

        var slot2 = pageManager.getSlotOffset(byteBuffer);
        assertTrue(slot2 < slot1);
    }

    @Test
    public void addTwoSlotsEachSlotReadable() {
        var free1 = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var r1 = new byte[ROW_SIZE_IN_BYTES];
        for (var i = 0; i < r1.length; i++) r1[i] = (byte) i;
        byteBuffer.put(free1, r1, 0, r1.length);
        pageManager.addSlot(byteBuffer, free1, r1.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free1);

        var free2 = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var r2 = new byte[ROW_SIZE_IN_BYTES];
        for (var i = 0; i < r2.length; i++) r2[i] = (byte) (i + 1);
        byteBuffer.put(free2, r2, 0, r2.length);
        pageManager.addSlot(byteBuffer, free2, r2.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free2);

        var out1 = new byte[ROW_SIZE_IN_BYTES];
        pageManager.getTupleFromSlotAt(page, 0).get(out1);
        assertArrayEquals(r1, out1);

        var out2 = new byte[ROW_SIZE_IN_BYTES];
        pageManager.getTupleFromSlotAt(page, 1).get(out2);
        assertArrayEquals(r2, out2);
    }

    @Test
    public void slotHeaderPositionsAreWrittenExactly() {
        int free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var rec = new byte[ROW_SIZE_IN_BYTES];
        for (var i = 0; i < rec.length; i++) {
            rec[i] = (byte) (i + 2);
        }

        byteBuffer.put(free, rec, 0, rec.length);
        pageManager.addSlot(byteBuffer, free, rec.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free);

        var slot0Location = PAGE_SIZE_IN_BYTES - SLOT_SIZE_IN_BYTES;
        var recordedOffset = byteBuffer.getInt(slot0Location);
        var recordedLength = byteBuffer.getInt(slot0Location + 4);

        assertEquals("Slot 0 should point to record offset", free, recordedOffset);
        assertEquals("Slot 0 should record length", rec.length, recordedLength);
    }

    @Test
    public void exhaustingSpaceDetectedAndFurtherWritesFail() {
        var adds = 0;
        while (pageManager.isSpaceAvailable(byteBuffer, pageManager.getFreeSpaceOffsetAddr(byteBuffer), ROW_SIZE_IN_BYTES)) {
            var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
            var rec = new byte[ROW_SIZE_IN_BYTES];
            pageManager.addSlot(byteBuffer, free, rec.length);
            byteBuffer.put(free, rec, 0, rec.length);
            pageManager.incrementFreeSpaceOffset(byteBuffer, free);
            adds++;
            if (adds > 1000) break;
        }

        var freeAfter = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        assertFalse("Page should report no space available after filling", pageManager.isSpaceAvailable(byteBuffer, freeAfter, ROW_SIZE_IN_BYTES));
    }

    @Test
    public void forcedAddAfterExhaustionOverlapsSlotArea() {
        while (pageManager.isSpaceAvailable(byteBuffer, pageManager.getFreeSpaceOffsetAddr(byteBuffer), ROW_SIZE_IN_BYTES)) {
            var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
            var rec = new byte[ROW_SIZE_IN_BYTES];
            pageManager.addSlot(byteBuffer, free, rec.length);
            byteBuffer.put(free, rec, 0, rec.length);
            pageManager.incrementFreeSpaceOffset(byteBuffer, free);
        }

        int freeAfter = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var extra = new byte[ROW_SIZE_IN_BYTES];
        try {
            byteBuffer.put(freeAfter, extra, 0, extra.length);
        } catch (IndexOutOfBoundsException ignored) {
            // JVMs might throw; that's acceptable for this test
        }

        pageManager.addSlot(byteBuffer, freeAfter, extra.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, freeAfter);

        int slotAfter = pageManager.getSlotOffset(byteBuffer);
        assertTrue("After forced add, data area should overlap slot area", freeAfter + extra.length > slotAfter);
    }

    @Test
    public void addingTooLargeRecordIsRejectedBySpaceCheck() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var slotOff = pageManager.getSlotOffset(byteBuffer);
        var available = slotOff - free;
        var tooLarge = new byte[available + 1];
        
        assertTrue("there should be some available space", available > 0);
        assertFalse("isSpaceAvailable should be false for oversize record", pageManager.isSpaceAvailable(byteBuffer, free, tooLarge.length));
        assertTrue("Oversize write would overlap slot area", free + tooLarge.length > slotOff);
    }
}
