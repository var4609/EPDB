package org.epdb.app

import org.epdb.engine.EngineModule.queryExecutor

fun main() {
    println("*************************************************")
    println("** Database System Startup (Phase 1: Read Core) **")
    println("*************************************************")

    val tableName = "users"
    val db = queryExecutor
    db.populateTestData(tableName)
//  db.executeSelectQuery("users");
//  db.executeSelectQueryWithFilter("users");
    db.executeSelectQueryWithFilterAndProjection(tableName)
}