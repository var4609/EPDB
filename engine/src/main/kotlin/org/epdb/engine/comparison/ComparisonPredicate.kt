package org.epdb.engine.comparison

import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.dto.Tuple
import org.epdb.engine.comparisonoperator.ComparisonOperator

data class ComparisonPredicate(
    val columnIndex: Int,
    val comparisonOperator: ComparisonOperator,
    val constantValue: ColumnValue
) : Predicate {

    override fun evaluate(tuple: Tuple): Boolean {
        val columnValue = tuple.getValueAtIndex(columnIndex)?: return false
        val comparison = columnValue.comparesTo(constantValue)

        return comparisonOperator.apply(comparison)
    }
}
