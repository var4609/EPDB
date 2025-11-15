package org.epdb.storage;

public class PageConstants {
    public static final int PAGE_SIZE_IN_BYTES = 4096;
    public static final int HEADER_SIZE_IN_BYTES = 8;
    public static final int HEADER_FREE_SPACE_OFFSET_ADDR = 0;
    public static final int HEADER_NUM_ROWS_ADDR = 4;
    public static final int SLOT_SIZE_IN_BYTES = 8;
    public static final int SLOT_RECORD_OFFSET_SIZE_IN_BYTES = 4;
    public static final int ROW_SIZE_IN_BYTES = 28;
    public static final int HEADER_NEXT_PAGE_ID_ADDR = 8;
    public static final int NO_NEXT_PAGE = -1;
}
