package org.epdb.buffer.manager

import org.epdb.buffer.dto.BufferFrame
import org.epdb.storage.dto.Page

interface BufferManager {
    fun getPage(pageId: Long, tableName: String): Page

    fun unpinPage(pageId: Long, isModified: Boolean)

    fun flushPage(bufferFrame: BufferFrame)

    fun allocateNewPage(tableName: String): Page
}