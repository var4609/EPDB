package org.epdb.org.epdb.commons

object Logger {

    @Volatile var enabled: Boolean = true

    fun info(message: String) {
        if(enabled) {
            println(message)
        }
    }

    fun error(message: String) {
        if(enabled) {
            System.err.println(message)
        }
    }

    fun enabled() {
        this.enabled = true
    }

    fun disabled() {
        this.enabled = false
    }
}