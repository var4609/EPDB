package org.epdb.engine.comparison;

import org.epdb.engine.dto.Tuple;

public interface Predicate {

    boolean evaluate(Tuple tuple);
}
