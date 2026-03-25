package org.epdb.storage

import org.epdb.catalog.CatalogModule
import org.epdb.storage.manager.InMemoryStorageManager
import org.epdb.storage.manager.StorageManager

object StorageModule {

    private val storageProvider: MutableMap<Long, ByteArray> = mutableMapOf()

    val storageManager: StorageManager by lazy {
        InMemoryStorageManager(storageProvider, CatalogModule.catalog)
    }
}