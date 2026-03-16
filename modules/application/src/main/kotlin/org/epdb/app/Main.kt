package org.epdb.app

import org.epdb.engine.EngineModule.queryExecutor
import org.epdb.engine.queryexecutor.InsertQueryExecutor
import org.epdb.org.epdb.commons.Logger

fun main(args: Array<String>) {
    Logger.info("*************************************************")
    Logger.info("** Database System Startup **")
    Logger.info("*************************************************")

    val tableName = "users"
    val db = queryExecutor

    if(args.isNotEmpty() && args.contains("--verify")) {
        Logger.info("* Verification mode enabled.")
    } else {
        val insertQueryExecutor = InsertQueryExecutor()
        insertQueryExecutor.populateTestData("users")
        db.executeSelectQueryWithFilterAndProjection(tableName)
    }
}