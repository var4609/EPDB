package org.epdb.buffer.dto;

import org.epdb.storage.dto.Page;

public record BufferFrame(Page page, boolean isDirty) {}
