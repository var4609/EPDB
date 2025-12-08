package org.epdb.buffer.manager

import org.epdb.buffer.dto.BufferFrame
import org.epdb.storage.dto.Page
import org.epdb.storage.manager.StorageManager

internal class InMemoryBufferManager(
    private val storageManager: StorageManager,
    private val bufferSize: Int,
    private val bufferFrames: MutableMap<Long, BufferFrame>
) : BufferManager {

    override fun getPage(pageId: Long): Page =
        bufferFrames.getOrPut(pageId) {
            if (bufferFrames.size >= bufferSize) {
                evictPage()
            }

            val newPage = storageManager.readPage(pageId)
            BufferFrame(newPage)
        }.also { it.pinCount++ }.page

    override fun unpinPage(pageId: Long, isModified: Boolean) {
        val bufferFrame = bufferFrames[pageId]?: run {
            System.err.println("Error: Page $pageId not found in buffer.")
            return
        }

        require (bufferFrame.pinCount > 0) {
            "Illegal unpin action on page ${pageId}. Pin count is already ${bufferFrame.pinCount}."
        }

        bufferFrame.apply {
            pinCount--
            isDirty = isDirty || isModified
        }
    }

    override fun flushPage(bufferFrame: BufferFrame) {
        if (bufferFrame.isDirty) {
            val page = bufferFrame.page
            storageManager.writePage(page.pageId, page.data)
            bufferFrame.isDirty = false
        }
    }

    override fun allocateNewPage(tableId: Int) = getPage(storageManager.allocatePage())

    private fun evictPage() {
        val victimKey = bufferFrames.keys.minOrNull()

        victimKey?.let { key ->
            val frameToEvict = bufferFrames.getValue(key)
            flushPage(frameToEvict)
            bufferFrames.remove(key)
        }
    }
}