package org.epdb.storage.manager

import org.epdb.storage.dto.Page

interface StorageManager {

    fun readPage(pageId: Long, tableName: String): Page

    fun writePage(pageId: Long, byteArray: ByteArray)

    fun allocatePage(tableName: String): Long

    fun getAllocatedPageCount(): Int
}