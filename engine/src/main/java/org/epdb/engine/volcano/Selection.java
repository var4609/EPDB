package org.epdb.engine.volcano;

import org.epdb.engine.comparison.Predicate;
import org.epdb.engine.dto.Tuple;

public record Selection(
        Predicate predicate,
        Operator childOperator
) implements Operator {

    @Override
    public void open() {
        this.childOperator.open();
        System.out.println("Opened child operator...");
    }

    @Override
    public Tuple next() {
        while (true) {
            var tuple = childOperator.next();

            if (tuple == null) {
                return null;
            }

            if (predicate.evaluate(tuple)) {
                return tuple;
            }
        }
    }

    @Override
    public void close() {
        this.childOperator.close();
        System.out.println("Closed child operator...");
    }

}
