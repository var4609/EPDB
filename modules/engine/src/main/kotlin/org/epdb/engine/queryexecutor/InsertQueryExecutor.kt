package org.epdb.engine.queryexecutor

import org.epdb.engine.EngineModule
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.dto.Tuple
import org.epdb.org.epdb.commons.Logger

class InsertQueryExecutor(
) {

    fun insertData(tuple: List<ColumnValue>) {
        // convert generic input to tuple the storage will accept (this is where inference is needed)
//        val tuple = Tuple(tuple.map { column ->
//            StringValue(column)
//        })

        val insertOperator = EngineModule.createInsertOperator(tupleToInsert = Tuple(tuple))

        insertOperator.use { op ->
            op.open()
            op.next()
        }
    }

    fun populateTestData(tableName: String) {
        Logger.info("\n--- Admin: Populating Test Data Directly to Storage ---")
        val rowCount = 5000

        for (i in 0..<rowCount) {
            val id = IntValue(i + 100)
            val name = StringValue("User_$i")
            val age = IntValue(20 + i)

            insertData(listOf(id, name, age))
        }
        Logger.info("--- Admin: Data population finished. ---")
    }
}