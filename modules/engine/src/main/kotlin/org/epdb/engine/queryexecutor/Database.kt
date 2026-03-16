package org.epdb.engine.queryexecutor

import org.epdb.engine.EngineModule
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.comparison.ComparisonPredicate
import org.epdb.engine.comparisonoperator.ComparisonOperator
import org.epdb.engine.databaseoperator.Operator
import org.epdb.engine.dto.Tuple
import org.epdb.org.epdb.commons.Logger

class Database {

    fun executeSelectQuery() {
        val scanOperator = EngineModule.createTableScanOperator()
        executeAndSink(scanOperator) { tuple -> Logger.info(tuple.toString()) }
    }

    fun executeSelectQueryWithFilter() {
        val scanOperator = EngineModule.createTableScanOperator()
        val predicate = ComparisonPredicate(0, ComparisonOperator.GREATER_THAN, IntValue(102))
        val filterOperator = EngineModule.createSelectionOperator(scanOperator, predicate)

        executeAndSink(filterOperator) { tuple -> Logger.info(tuple.toString()) }
    }

    fun executeSelectQueryWithFilterAndProjection(searchKey: Int, projectionColumns: Set<Int>) : List<Tuple> {
        val indexScanOperator = EngineModule.createIndexScanOperator(IntValue(searchKey))
        val predicate = ComparisonPredicate(0, ComparisonOperator.EQUALS, IntValue(searchKey))
        val filterOperator = EngineModule.createSelectionOperator(indexScanOperator, predicate)
        val projectionOperator = EngineModule.createProjectionOperator(filterOperator, projectionColumns)

        val result = mutableListOf<Tuple>()
        executeAndSink(projectionOperator) { tuple -> result.add(tuple) }

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
