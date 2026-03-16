package org.epdb.engine.dto

enum class ColumnType(val sizeInBytes: Int) {
    INT(4),
    STRING_FIXED_TYPE(20)
}