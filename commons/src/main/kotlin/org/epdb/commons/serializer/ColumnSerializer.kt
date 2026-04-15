package org.epdb.org.epdb.commons.serializer

import org.epdb.commons.engine.ColumnType
import org.epdb.org.epdb.commons.engine.ColumnValue
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.math.min

interface ColumnSerializer<T: ColumnValue> {

    fun serialize(value: T, type: ColumnType, byteBuffer: ByteBuffer)
}

object IntColumnSerializer : ColumnSerializer<ColumnValue.IntValue> {
    override fun serialize(value: ColumnValue.IntValue, type: ColumnType, byteBuffer: ByteBuffer) {
        byteBuffer.putInt(value.value)
    }
}

object StringColumnSerializer : ColumnSerializer<ColumnValue.StringValue> {
    override fun serialize(value: ColumnValue.StringValue, type: ColumnType, byteBuffer: ByteBuffer) {
        val maxStringSize = type.sizeInBytes
        val nameBytes = value.value.toByteArray(StandardCharsets.UTF_8)
        val writeLength = min(maxStringSize, nameBytes.size)

        byteBuffer.put(nameBytes, 0, writeLength)
        repeat(maxStringSize - writeLength) {
            byteBuffer.put(0.toByte())
        }
    }
}