package org.epdb.engine.comparison

import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.dto.Tuple
import org.epdb.engine.comparisonoperator.Operator

data class ComparisonPredicate(
    val columnIndex: Int,
    val operator: Operator,
    val constantValue: ColumnValue
) : Predicate {

    override fun evaluate(tuple: Tuple): Boolean {
        val columnValue = tuple.getValueAtIndex(columnIndex)?: return false
        val comparison = columnValue.comparesTo(constantValue)

        return operator.apply(comparison)
    }
}
