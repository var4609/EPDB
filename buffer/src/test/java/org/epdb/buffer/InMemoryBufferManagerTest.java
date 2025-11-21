package org.epdb.buffer;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

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
    private static final Map<Long, BufferFrame> MAP_WITH_SINGLE_FRAME = new HashMap<>();

    private final StorageManager storageManager = mock();
    private final Map<Long, BufferFrame> bufferFrames = mock();

    private InMemoryBufferManager bufferManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        bufferManager = new InMemoryBufferManager(storageManager, BUFFER_SIZE, bufferFrames);
    }

    @Test
    public void getPage_cachesResult() {
        when(storageManager.readPage(1L)).thenReturn(PAGE);

        var first = bufferManager.getPage(1L);
        var second = bufferManager.getPage(1L);

        assertSame(first, second);
        verify(storageManager, times(2)).readPage(1L);
    }

    @Test
    public void flushPage_writesToStorage() {
        bufferManager.flushPage(PAGE);

        verify(storageManager, times(1)).writePage(1L, PAGE.getData());
    }

    @Test
    public void eviction_writesDirtyVictim() throws Exception {
        int bufferSize = 1;
        MAP_WITH_SINGLE_FRAME.put(0L, new BufferFrame(PAGE, true));
        bufferManager = new InMemoryBufferManager(storageManager, bufferSize, MAP_WITH_SINGLE_FRAME);

        when(storageManager.readPage(10L)).thenReturn(new Page(10L, new byte[] {(byte)10}));

        bufferManager.getPage(10L);

        verify(storageManager, atLeastOnce()).writePage(PAGE.getPageId(), PAGE.getData());
    }
}
