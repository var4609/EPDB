package org.epdb.storage

public interface Storage {
    fun store(data: ByteArray, identifier: String)
    fun retrieve(identifier: String): ByteArray?
}