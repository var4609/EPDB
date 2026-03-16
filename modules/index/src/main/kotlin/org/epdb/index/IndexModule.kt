package org.epdb.index

import org.epdb.index.dto.PagePointer
import org.epdb.index.manager.InMemoryIndexManager
import org.epdb.index.manager.IndexManager

object IndexModule {

    private val indexStore: MutableMap<Any, MutableList<PagePointer>> by lazy {
        mutableMapOf()
    }

    val indexManager : IndexManager by lazy {
        InMemoryIndexManager(indexStore)
    }
}