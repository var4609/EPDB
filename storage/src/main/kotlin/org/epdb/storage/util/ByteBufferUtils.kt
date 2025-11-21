package org.epdb.storage.util

import org.epdb.storage.pagemanager.PageConstants.*
import java.nio.ByteBuffer
import java.nio.ByteOrder

fun ByteBuffer.initializePageWithHeader(
    freeSpaceOffsetValue: Int,
    numRowsValue: Int = 0,
    nextPageIdValue: Int = NO_NEXT_PAGE
): ByteBuffer = this.apply {
    order(ByteOrder.LITTLE_ENDIAN)
    putInt(HEADER_FREE_SPACE_OFFSET_ADDR, freeSpaceOffsetValue)
    putInt(HEADER_NUM_ROWS_ADDR, numRowsValue)
    putInt(HEADER_NEXT_PAGE_ID_ADDR, nextPageIdValue)
}