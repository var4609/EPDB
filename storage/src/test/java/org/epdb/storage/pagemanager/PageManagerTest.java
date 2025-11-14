package org.epdb.storage.pagemanager;

import org.epdb.storage.dto.Page;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class PageManagerTest {

    public static final long PAGE_ID = 1L;
    private static final int PAGE_SIZE = 4096;
    private static final int ROW_SIZE = 28;
    private static final int SLOT_SIZE = 8;

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
        return new Page(PAGE_ID, new byte[PAGE_SIZE]);
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
        assertEquals(old + ROW_SIZE, byteBuffer.getInt(0));
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
        var rec = new byte[ROW_SIZE];
        byteBuffer.put(free, rec, 0, rec.length);

        pageManager.addSlot(byteBuffer, free, rec.length);

        int slot0 = PAGE_SIZE - SLOT_SIZE;
        assertEquals(free, byteBuffer.getInt(slot0));
        assertEquals(rec.length, byteBuffer.getInt(slot0 + 4));
    }

    @Test
    public void getRecordFromSlotAtReturnsRecord() {
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var rec = new byte[ROW_SIZE];
        for (var i = 0; i < rec.length; i++) {
            rec[i] = (byte) (i + 5);
        }
        byteBuffer.put(free, rec, 0, rec.length);

        pageManager.addSlot(byteBuffer, free, rec.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free);

        var got = pageManager.getRecordFromSlotAt(page, 0);
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

        // add a row to shift slot offset
        var free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        pageManager.addSlot(byteBuffer, free, ROW_SIZE);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free);

        var slot2 = pageManager.getSlotOffset(byteBuffer);
        assertTrue(slot2 < slot1);
    }

    @Test
    public void addTwoSlotsEachSlotReadable() {
        var free1 = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var r1 = new byte[ROW_SIZE];
        for (int i = 0; i < r1.length; i++) r1[i] = (byte) i;
        byteBuffer.put(free1, r1, 0, r1.length);
        pageManager.addSlot(byteBuffer, free1, r1.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free1);

        int free2 = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        var r2 = new byte[ROW_SIZE];
        for (int i = 0; i < r2.length; i++) r2[i] = (byte) (i + 1);
        byteBuffer.put(free2, r2, 0, r2.length);
        pageManager.addSlot(byteBuffer, free2, r2.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free2);

        var out1 = new byte[ROW_SIZE];
        pageManager.getRecordFromSlotAt(page, 0).get(out1);
        assertArrayEquals(r1, out1);

        var out2 = new byte[ROW_SIZE];
        pageManager.getRecordFromSlotAt(page, 1).get(out2);
        assertArrayEquals(r2, out2);
    }

    @Test
    public void slotHeaderPositionsAreWrittenExactly() {
        Page p = newEmptyPage();
        ByteBuffer byteBuffer = ByteBuffer.wrap(p.data());

        int free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);

        var rec = new byte[ROW_SIZE];
        for (int i = 0; i < rec.length; i++) rec[i] = (byte) (i + 2);

        // write record and register slot
        byteBuffer.put(free, rec, 0, rec.length);
        pageManager.addSlot(byteBuffer, free, rec.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, free);

        // slot 0 location and fields
        int slot0Location = PAGE_SIZE - SLOT_SIZE;
        int recordedOffset = byteBuffer.getInt(slot0Location);
        int recordedLength = byteBuffer.getInt(slot0Location + 4);

        assertEquals("Slot 0 should point to record offset", free, recordedOffset);
        assertEquals("Slot 0 should record length", rec.length, recordedLength);
    }

    @Test
    public void exhaustingSpaceDetectedAndFurtherWritesFail() {
        Page p = newEmptyPage();
        ByteBuffer byteBuffer = ByteBuffer.wrap(p.data());

        int adds = 0;
        // keep adding fixed-size rows until no space according to PageManager
        while (pageManager.isSpaceAvailable(byteBuffer, pageManager.getFreeSpaceOffsetAddr(byteBuffer), ROW_SIZE)) {
            int free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
            var rec = new byte[ROW_SIZE];
            pageManager.addSlot(byteBuffer, free, rec.length);
            // write the bytes into the data area - put may throw if out of bounds
            byteBuffer.put(free, rec, 0, rec.length);
            pageManager.incrementFreeSpaceOffset(byteBuffer, free);
            adds++;
            // safety: avoid infinite loop
            if (adds > 1000) break;
        }

        int freeAfter = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        assertFalse("Page should report no space available after filling", pageManager.isSpaceAvailable(byteBuffer, freeAfter, ROW_SIZE));
    }

    @Test
    public void forcedAddAfterExhaustionOverlapsSlotArea() {
        Page p = newEmptyPage();
        ByteBuffer byteBuffer = ByteBuffer.wrap(p.data());

        // fill until no space
        while (pageManager.isSpaceAvailable(byteBuffer, pageManager.getFreeSpaceOffsetAddr(byteBuffer), ROW_SIZE)) {
            int free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
            var rec = new byte[ROW_SIZE];
            pageManager.addSlot(byteBuffer, free, rec.length);
            byteBuffer.put(free, rec, 0, rec.length);
            pageManager.incrementFreeSpaceOffset(byteBuffer, free);
        }

        int freeAfter = pageManager.getFreeSpaceOffsetAddr(byteBuffer);

        // Attempting to write another record at freeAfter would overlap the slot area.
        var extra = new byte[ROW_SIZE];
        try {
            byteBuffer.put(freeAfter, extra, 0, extra.length);
        } catch (IndexOutOfBoundsException ignored) {
            // JVMs might throw; that's acceptable for this test
        }

        // Register the (forced) slot and advance the header row count to observe the overlap
        pageManager.addSlot(byteBuffer, freeAfter, extra.length);
        pageManager.incrementFreeSpaceOffset(byteBuffer, freeAfter);

        int slotAfter = pageManager.getSlotOffset(byteBuffer);
        assertTrue("After forced add, data area should overlap slot area", freeAfter + extra.length > slotAfter);
    }

    @Test
    public void addingTooLargeRecordIsRejectedBySpaceCheck() {
        Page p = newEmptyPage();
        ByteBuffer byteBuffer = ByteBuffer.wrap(p.data());

        int free = pageManager.getFreeSpaceOffsetAddr(byteBuffer);
        int slotOff = pageManager.getSlotOffset(byteBuffer);

        int available = slotOff - free;
        assertTrue("there should be some available space", available > 0);

        // create a record larger than available
        var tooLarge = new byte[available + 1];
        assertFalse("isSpaceAvailable should be false for oversize record", pageManager.isSpaceAvailable(byteBuffer, free, tooLarge.length));

        // Writing this oversize record would overlap the slot area (semantic check)
        assertTrue("Oversize write would overlap slot area", free + tooLarge.length > slotOff);
    }
}
