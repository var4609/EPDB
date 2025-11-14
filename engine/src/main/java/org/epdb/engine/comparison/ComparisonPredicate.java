package org.epdb.engine.comparison;

import org.epdb.engine.dto.ColumnValue;
import org.epdb.engine.dto.Tuple;

public record ComparisonPredicate(
        int columnIndex,
        Op operator,
        ColumnValue constantValue
) implements Predicate {

    @Override
    public boolean evaluate(Tuple tuple) {
        var columnValue = tuple.getValueAtIndex(this.columnIndex);
        if (!(this.constantValue instanceof ColumnValue constantColumnValue)) {
            return false;
        }

        try {
            int comparison = columnValue.comparesTo(constantColumnValue);
            return this.operator.apply(comparison);
        } catch (Exception e) {
            return false;
        }
    }
}
