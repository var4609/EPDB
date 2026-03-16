package org.epdb.storage

import org.epdb.storage.manager.InMemoryStorageManager
import org.epdb.storage.manager.StorageManager

object StorageModule {

    private const val NEXT_PAGE_ID = 0L
    private val storageProvider: MutableMap<Long, ByteArray> = mutableMapOf()

    val storageManager: StorageManager by lazy {
        InMemoryStorageManager(
            storageProvider = storageProvider,
            nextPageId = NEXT_PAGE_ID
        )
    }
}