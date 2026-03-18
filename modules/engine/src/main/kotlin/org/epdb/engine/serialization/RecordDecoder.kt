package org.epdb.engine.serialization

import java.nio.ByteBuffer
import java.nio.BufferUnderflowException
import java.nio.ByteOrder
import org.epdb.commons.engine.Schema
import org.epdb.commons.engine.ColumnType
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.org.epdb.commons.Logger

object RecordDecoder {

    fun deserialize(buffer: ByteBuffer, recordSchema: Schema) : List<ColumnValue> {
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        return try {
            recordSchema.columnDefinitions.map { column ->
                when(column.columnType) {
                    ColumnType.INT -> {
                        IntValue(buffer.getInt())
                    }
                    ColumnType.STRING_FIXED_TYPE -> {
                        val nameBytes = ByteArray(ColumnType.STRING_FIXED_TYPE.sizeInBytes)
                        buffer.get(nameBytes)
                        StringValue(String(nameBytes).trim())
                    }
                }
            }
        } catch (e: BufferUnderflowException) {
            Logger.error("Decoding record failed with exception $e. Returning empty record")
            emptyList()
        }
    }
}