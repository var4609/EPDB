package org.epdb.engine.volcano;

import java.util.Set;

import org.epdb.engine.dto.Tuple;

public class Projection implements Operator {

    private final Operator childOperator;
    private final Set<Integer> projectionColumns;

    public Projection(final Operator childOperator, final Set<Integer> projectionColumns) {
        this.childOperator = childOperator;
        this.projectionColumns = projectionColumns;
    }

    @Override
    public void open() {
        this.childOperator.open();
        System.out.println("Opening child operator...");
    }

    @Override
    public Tuple next() {
        while (true) {
            var tuple = childOperator.next();

            if(tuple == null) {
                return tuple;
            }

            Object[] values = new Object[this.projectionColumns.size()];
            int k = 0;
            for(var i=0; i<tuple.values().length; i++) {
                if(this.projectionColumns.contains(i)) {
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
