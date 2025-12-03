package org.epdb.buffer

import org.epdb.buffer.dto.BufferFrame
import org.epdb.storage.dto.Page

interface BufferManager {
    fun getPage(pageId: Long): Page

    fun unpinPage(pageId: Long, isModified: Boolean)

    fun flushPage(bufferFrame: BufferFrame)

    fun allocateNewPage(tableId: Int): Page
}
