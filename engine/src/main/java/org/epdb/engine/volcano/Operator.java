package org.epdb.engine.volcano;

import org.epdb.engine.dto.Tuple;

public interface Operator {
    
    void open();

    Tuple next();

    void close();
}
