package org.epdb.buffer;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.epdb.buffer.dto.BufferFrame;
import org.epdb.storage.manager.StorageManager;
import org.epdb.storage.dto.Page;

public class InMemoryBufferManagerTest {

    private static final int BUFFER_SIZE = 10;
    private static final Page PAGE = new Page(1L, new byte[] {1,2,3});
    private static final BufferFrame CLEAN_BUFFER_FRAME = new BufferFrame(PAGE, false, 0);
    private static final BufferFrame DIRTY_BUFFER_FRAME_ONE = new BufferFrame(PAGE, true, 0);
    private static final BufferFrame DIRTY_BUFFER_FRAME_TWO = new BufferFrame(PAGE, true, 0);

    private final StorageManager storageManager = mock();
    private final Map<Long, BufferFrame> bufferFrames = mock();

    private InMemoryBufferManager bufferManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        bufferManager = new InMemoryBufferManager(storageManager, BUFFER_SIZE, bufferFrames);
    }

    @Test
    public void testStorageManagerIsNotCalledForCachedPages() {
        var pageId = 1L;
        when(bufferFrames.containsKey(pageId)).thenReturn(false).thenReturn(true);
        when(bufferFrames.get(pageId)).thenReturn(CLEAN_BUFFER_FRAME);
        when(storageManager.readPage(pageId)).thenReturn(PAGE);

        var first = bufferManager.getPage(pageId);
        var second = bufferManager.getPage(pageId);

        assertSame(first, second);
        verify(storageManager, times(1)).readPage(pageId);
    }

    @Test
    public void flushPageWritesToStorage() {
        bufferManager.flushPage(DIRTY_BUFFER_FRAME_ONE);
        verify(storageManager, times(1)).writePage(1L, PAGE.getData());
    }

    @Test
    public void evictionWritesDirtyVictim() {
        var newPageId = 10L;
        var victimPageId = PAGE.getPageId();
        when(bufferFrames.containsKey(newPageId)).thenReturn(false);
        when(bufferFrames.size()).thenReturn(BUFFER_SIZE);
        when(bufferFrames.keySet()).thenReturn(Set.of(victimPageId, newPageId));
        when(bufferFrames.get(victimPageId)).thenReturn(DIRTY_BUFFER_FRAME_TWO);
        when(storageManager.readPage(newPageId)).thenReturn(new Page(newPageId, new byte[] {(byte)10}));

        bufferManager.getPage(newPageId);

        verify(storageManager, atLeastOnce()).writePage(PAGE.getPageId(), PAGE.getData());
    }
}
