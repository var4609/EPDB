package org.epdb.engine.volcano;

import java.util.Set;

import org.epdb.engine.dto.ColumnValue;
import org.epdb.engine.dto.Tuple;

public record Projection(
        Operator childOperator,
        Set<Integer> projectionColumns
) implements Operator {

    @Override
    public void open() {
        this.childOperator.open();
    }

    @Override
    public Tuple next() {
        var tuple = childOperator.next();

        if (tuple == null) {
            return null;
        }

        ColumnValue[] values = new ColumnValue[this.projectionColumns.size()];
        int k = 0;
        for (var i = 0; i < tuple.values().length; i++) {
            if (this.projectionColumns.contains(i)) {
                values[k++] = tuple.getValueAtIndex(i);
            }
        }

        return new Tuple(values);
    }

    @Override
    public void close() {
        this.childOperator.close();
    }
}
