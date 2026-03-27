package org.epdb.engine.comparison

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import org.epdb.engine.columntypes.ColumnValue
import org.epdb.engine.comparisonoperator.ComparisonOperator
import org.epdb.engine.dto.Tuple

class ComparisonPredicateTest : BehaviorSpec({

    val columnIndex = 0
    val tupleValue = 100
    val TEST_TUPLE = Tuple(listOf(ColumnValue.IntValue(tupleValue)))

    Given("A ComparisonPredicate for index 0 (value $tupleValue)") {

        When("The operator is EQUALS and the values are equal") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.EQUALS, ColumnValue.IntValue(tupleValue))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is EQUALS and the values are not equal") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.EQUALS, ColumnValue.IntValue(tupleValue - 5))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is NOT_EQUALS and the values are not equal") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.NOT_EQUALS, ColumnValue.IntValue(tupleValue - 5))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is NOT_EQUALS and the values are equal") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.NOT_EQUALS, ColumnValue.IntValue(tupleValue))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is GREATER_THAN ($tupleValue > ${tupleValue - 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.GREATER_THAN, ColumnValue.IntValue(tupleValue - 5))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is GREATER_THAN ($tupleValue < ${tupleValue + 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.GREATER_THAN, ColumnValue.IntValue(tupleValue + 5))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is GREATER_THAN ($tupleValue = $tupleValue") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.GREATER_THAN, ColumnValue.IntValue(tupleValue))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is LESS_THAN ($tupleValue > ${tupleValue - 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.LESS_THAN, ColumnValue.IntValue(tupleValue - 5))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is LESS_THAN ($tupleValue < ${tupleValue + 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.LESS_THAN, ColumnValue.IntValue(tupleValue + 5))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is LESS_THAN ($tupleValue = $tupleValue") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.LESS_THAN, ColumnValue.IntValue(tupleValue))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is GREATER_THAN_OR_EQUALS ($tupleValue > ${tupleValue - 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.GREATER_THAN_OR_EQUALS, ColumnValue.IntValue(tupleValue - 5))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is GREATER_THAN_OR_EQUALS ($tupleValue < ${tupleValue + 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.GREATER_THAN_OR_EQUALS, ColumnValue.IntValue(tupleValue + 5))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is GREATER_THAN_OR_EQUALS ($tupleValue = $tupleValue") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.GREATER_THAN_OR_EQUALS, ColumnValue.IntValue(tupleValue))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is LESS_THAN_OR_EQUALS ($tupleValue > ${tupleValue - 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.LESS_THAN_OR_EQUALS, ColumnValue.IntValue(tupleValue - 5))
            Then("evaluate() should return false") {
                predicate.evaluate(TEST_TUPLE) shouldBe false
            }
        }

        When("The operator is LESS_THAN_OR_EQUALS ($tupleValue < ${tupleValue + 5}") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.LESS_THAN_OR_EQUALS, ColumnValue.IntValue(tupleValue + 5))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }

        When("The operator is LESS_THAN_OR_EQUALS ($tupleValue = $tupleValue") {
            val predicate = ComparisonPredicate(columnIndex, ComparisonOperator.LESS_THAN_OR_EQUALS, ColumnValue.IntValue(tupleValue))
            Then("evaluate() should return true") {
                predicate.evaluate(TEST_TUPLE) shouldBe true
            }
        }
    }

})
