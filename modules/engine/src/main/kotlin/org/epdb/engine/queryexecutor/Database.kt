package org.epdb.engine.queryexecutor

import org.epdb.engine.EngineModule
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.comparison.ComparisonPredicate
import org.epdb.engine.comparisonoperator.ComparisonOperator
import org.epdb.engine.databaseoperator.Operator
import org.epdb.engine.dto.Tuple
import org.epdb.org.epdb.commons.Logger

class Database {

    fun executeSelectQuery(tableName: String) {
        val scanOperator = EngineModule.createTableScanOperator(tableName)
        executeAndSink(scanOperator) { tuple -> Logger.info(tuple.toString()) }
    }

    fun executeSelectQueryWithFilter(tableName: String) {
        val scanOperator = EngineModule.createTableScanOperator(tableName)
        val predicate = ComparisonPredicate(0, ComparisonOperator.GREATER_THAN, ColumnValue.IntValue(102))
        val filterOperator = EngineModule.createSelectionOperator(scanOperator, predicate)

        executeAndSink(filterOperator) { tuple -> Logger.info(tuple.toString()) }
    }

    fun executeSelectQueryWithFilterAndProjection(searchKey: Int, projectionColumns: Set<Int>) : List<Tuple> {
        val indexScanOperator = EngineModule.createIndexScanOperator(ColumnValue.IntValue(searchKey))
        val predicate = ComparisonPredicate(0, ComparisonOperator.EQUALS, ColumnValue.IntValue(searchKey))
        val filterOperator = EngineModule.createSelectionOperator(indexScanOperator, predicate)
        val projectionOperator = EngineModule.createProjectionOperator(filterOperator, projectionColumns)

        val result = mutableListOf<Tuple>()
        executeAndSink(projectionOperator) { tuple -> Logger.info(tuple.toString()) }

        return result
    }

    private fun executeAndSink(rootOperator: Operator, sink: (Tuple) -> Unit) {
        rootOperator.use { op ->
            op.open()
            var tuple: Tuple?

            while (op.next().also { tuple = it } != null) {
                sink(tuple!!)
            }
        }
    }
}
