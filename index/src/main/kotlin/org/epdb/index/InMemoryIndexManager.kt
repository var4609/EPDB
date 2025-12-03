package org.epdb.index

import org.epdb.index.dto.PagePointer

class InMemoryIndexManager(
    private val indexStore: MutableMap<Any, MutableList<PagePointer>> = mutableMapOf()
) : IndexManager {

    override fun addEntry(key: Any, pageId: Long, slotIndex: Int) {
        val pagePointer = PagePointer(pageId, slotIndex);
        indexStore.computeIfAbsent(key) { mutableListOf() }.add(pagePointer)
    }

    override fun removeEntry(key: Any, pageId: Long, slotIndex: Int) : Boolean {
        val pagePointers = indexStore[key] ?: return false
        val pagePointerToRemove = PagePointer(pageId, slotIndex)

        return pagePointers.remove(pagePointerToRemove)
    }

    override fun lookupEntry(key: Any): List<PagePointer> {
        return indexStore.getOrDefault(key, emptyList())
    }
}