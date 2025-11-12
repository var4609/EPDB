package org.epdb.engine.comparison;

import org.epdb.engine.dto.Tuple;

public class ComparisonPredicate implements Predicate {
    
    private final int columnIndex;
    private final Op operator;
    private final Object constantValue;

    public ComparisonPredicate(int columnIndex, Op operator, Object constantValue) {
        this.columnIndex = columnIndex;
        this.operator = operator;
        this.constantValue = constantValue;
    }

    @Override
    public boolean evaluate(Tuple tuple) {

        var columnValue = tuple.getValueAtIndex(this.columnIndex);

        if(!columnValue.getClass().equals(this.constantValue.getClass())) {
            return false;
        }

        if(columnValue instanceof String) {
            var leftOperand = (String) columnValue;
            var rightOperand = (String) this.constantValue;
            var comparison = leftOperand.compareTo(rightOperand);

            return switch(this.operator) {
                case EQUALS -> comparison == 0;
                case GREATER_THAN -> comparison > 0;
                case GREATER_THAN_OR_EQUALS -> comparison >= 0;
                case LESS_THAN -> comparison < 0;
                case LESS_THAN_OR_EQUALS -> comparison <= 0;
                case NOT_EQUALS -> comparison != 0;
            };
        } else if(columnValue instanceof Integer) {
            var leftOperand = (int) columnValue;
            var rightOperand = (int) this.constantValue;

            return switch(this.operator) {
                case EQUALS -> leftOperand == rightOperand;
                case GREATER_THAN -> leftOperand > rightOperand;
                case GREATER_THAN_OR_EQUALS -> leftOperand >= rightOperand;
                case LESS_THAN -> leftOperand < rightOperand;
                case LESS_THAN_OR_EQUALS -> leftOperand <= rightOperand;
                case NOT_EQUALS -> leftOperand != rightOperand;
            };
        }

        return false;
    }
    
}
