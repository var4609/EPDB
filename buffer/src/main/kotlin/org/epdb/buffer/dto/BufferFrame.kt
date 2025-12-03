package org.epdb.buffer.dto

import org.epdb.storage.dto.Page

class BufferFrame(
    val page: Page,
    var isDirty: Boolean = false,
    var pinCount: Int = 0
)
