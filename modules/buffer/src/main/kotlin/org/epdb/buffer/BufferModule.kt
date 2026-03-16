package org.epdb.buffer

import org.epdb.buffer.dto.BufferFrame
import org.epdb.buffer.manager.BufferManager
import org.epdb.buffer.manager.InMemoryBufferManager
import org.epdb.storage.StorageModule

object BufferModule {

    private const val BUFFER_SIZE: Int = 50

    private val bufferFrames: MutableMap<Long, BufferFrame> = mutableMapOf()

    val bufferManager : BufferManager by lazy {
        InMemoryBufferManager(
            storageManager = StorageModule.storageManager,
            bufferSize = BUFFER_SIZE,
            bufferFrames = bufferFrames
        )
    }
}