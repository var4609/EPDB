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
        System.out.println("Opening child operator...");
    }

    @Override
    public Tuple next() {
        while (true) {
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
    }

    @Override
    public void close() {
        this.childOperator.close();
        System.out.println("Closed child operator...");
    }
}
