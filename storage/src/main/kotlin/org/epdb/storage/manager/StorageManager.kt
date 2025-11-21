package org.epdb.storage.manager

import org.epdb.storage.dto.Page

interface StorageManager {

    fun readPage(pageId: Long): Page

    fun writePage(pageId: Long, byteArray: ByteArray)

    fun allocatePage(): Long

    fun getAllocatedPageCount(): Int
}