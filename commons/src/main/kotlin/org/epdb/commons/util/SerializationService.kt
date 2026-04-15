package org.epdb.org.epdb.commons.util

import org.epdb.commons.engine.ColumnType
import org.epdb.org.epdb.commons.engine.ColumnValue
import org.epdb.org.epdb.commons.serializer.IntColumnSerializer
import org.epdb.org.epdb.commons.serializer.StringColumnSerializer
import java.nio.ByteBuffer

object SerializationService {

    fun serialize(columnValue: ColumnValue, columnType: ColumnType, byteBuffer: ByteBuffer) {
        when (columnValue) {
            is ColumnValue.IntValue -> {
                validate(columnType, ColumnType.INT)
                IntColumnSerializer.serialize(columnValue, columnType, byteBuffer)
            }
            is ColumnValue.StringValue -> {
                validate(columnType, ColumnType.STRING_FIXED_TYPE)
                StringColumnSerializer.serialize(columnValue, columnType, byteBuffer)
            }
        }
    }

    private fun validate(actual: ColumnType, expected: ColumnType) {
        if(actual != expected) { throw IllegalArgumentException("Expected $expected but found $actual") }
    }
}