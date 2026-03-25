package org.epdb.storage.util

object GlobalPageAllocator {

    //TODO: Persist on disk
    @Volatile
    var nextPageId: Long = -1
        get() {
            field++
            return field
        }
}