package org.epdb.storage.util

object PageConstants {
    const val PAGE_SIZE_IN_BYTES: Int = 4096
    const val HEADER_SIZE_IN_BYTES: Int = 12
    const val HEADER_FREE_SPACE_OFFSET_ADDR: Int = 0
    const val HEADER_NUM_ROWS_ADDR: Int = 4
    const val HEADER_NEXT_PAGE_ID_ADDR: Int = 8
    const val SLOT_SIZE_IN_BYTES: Int = 8
    const val SLOT_RECORD_OFFSET_SIZE_IN_BYTES: Int = 4
    const val ROW_SIZE_IN_BYTES: Int = 28
    const val NO_NEXT_PAGE: Int = -1
}