package org.epdb.engine.volcano;

import java.util.List;
import java.util.Set;

import org.epdb.engine.columntypes.ColumnValue;
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
        for (var i = 0; i < tuple.getValues().size(); i++) {
            if (this.projectionColumns.contains(i)) {
                values[k++] = tuple.getValueAtIndex(i);
            }
        }

        return new Tuple(List.of(values));
    }

    @Override
    public void close() {
        this.childOperator.close();
    }
}
