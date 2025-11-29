package org.epdb.buffer.dto;

import org.epdb.storage.dto.Page;

public class BufferFrame {

    public Page page;
    public boolean isDirty;
    public int pinCount;

    public BufferFrame(Page page, boolean isDirty, int pinCount) {
        this.page = page;
        this.isDirty = isDirty;
        this.pinCount = pinCount;
    }

    public BufferFrame(Page page) {
        this(page, false, 0);
    }
}
