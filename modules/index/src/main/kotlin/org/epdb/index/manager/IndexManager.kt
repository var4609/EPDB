package org.epdb.index.manager

import org.epdb.index.dto.PagePointer

interface IndexManager {

    fun addEntry(key: Any, pageId: Long, slotIndex: Int)

    fun removeEntry(key: Any, pageId: Long, slotIndex: Int) : Boolean

    fun lookupEntry(key: Any) : List<PagePointer>
}