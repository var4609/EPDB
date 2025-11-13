package org.epdb.engine.dto;

public sealed interface ColumnValue permits IntValue, StringValue {

    int comparesTo(ColumnValue other);
}
