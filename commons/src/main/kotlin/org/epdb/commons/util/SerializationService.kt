package org.epdb.org.epdb.commons.util

import org.epdb.commons.engine.ColumnType
import org.epdb.commons.engine.Schema
import org.epdb.org.epdb.commons.Logger
import org.epdb.org.epdb.commons.engine.ColumnValue
import org.epdb.org.epdb.commons.serializer.IntColumnSerializer
import org.epdb.org.epdb.commons.serializer.StringColumnSerializer
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.nio.ByteOrder

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

    fun deserialize(byteBuffer: ByteBuffer, schema: Schema) : List<ColumnValue> {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
        return try {
            schema.columnDefinitions.map { column ->
                try {
                    when(column.columnType) {
                        ColumnType.INT -> {
                            ColumnValue.IntValue(byteBuffer.getInt())
                        }
                        ColumnType.STRING_FIXED_TYPE -> {
                            val nameBytes = ByteArray(column.columnType.sizeInBytes)
                            byteBuffer.get(nameBytes)

                            val rawString = String(nameBytes, Charsets.UTF_8)
                            ColumnValue.StringValue(rawString.trim())
                        }
                    }
                } catch (e: BufferUnderflowException) {
                    throw IllegalStateException("Could not decode column type: ${column.columnType}", e)
                }
            }
        } catch (e: IllegalArgumentException) {
            Logger.error("Decoding record failed with exception $e. Returning empty record")
            emptyList()
        }
    }

    private fun validate(actual: ColumnType, expected: ColumnType) {
        if(actual != expected) { throw IllegalArgumentException("Expected $expected but found $actual") }
    }
}