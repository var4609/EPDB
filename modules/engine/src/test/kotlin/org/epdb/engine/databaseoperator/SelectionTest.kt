package org.epdb.engine.databaseoperator

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.comparison.Predicate
import org.epdb.engine.dto.Tuple

class SelectionTest : BehaviorSpec({

    val TUPLE_MATCH = Tuple(listOf(IntValue(10)))
    val TUPLE_NO_MATCH = Tuple(listOf(IntValue(5)))
    val TUPLE_OTHER_MATCH = Tuple(listOf(IntValue(20)))

    val mockChildOperator : Operator = mockk(relaxed = true)
    val mockPredicate : Predicate = mockk()
    lateinit var selection: Selection

    beforeContainer {
        clearAllMocks()
        every { mockChildOperator.open() } just Runs
        every { mockChildOperator.close() } just Runs
    }

    Given("A Selection Operator") {
        selection = Selection(mockPredicate, mockChildOperator)

        When("open() is called") {
            selection.open()

            Then("it should open the child operator") {
                verify(exactly = 1) {
                    mockChildOperator.open()
                }
            }
        }

        When("close() is called") {
            selection.close()

            Then("it should close the child operator") {
                verify(exactly = 1) {
                    mockChildOperator.close()
                }
            }
        }
    }

    Given("A Selection operator with a child that yields a single matching tuple") {
        selection = Selection(mockPredicate, mockChildOperator)

        When("next() is called") {
            every { mockChildOperator.next() } returns TUPLE_MATCH andThen null
            every { mockPredicate.evaluate(TUPLE_MATCH) } returns true
            val result = selection.next()

            Then("it should return the matching tuple") {
                result shouldBe TUPLE_MATCH
            }

            Then("it should evaluate the predicate once") {
                verify(exactly = 1) { mockPredicate.evaluate(TUPLE_MATCH) }
            }
        }
    }

    Given("A Selection operator with mixed tuples") {
        selection = Selection(mockPredicate, mockChildOperator)

        When("next() is called three times") {
            every { mockChildOperator.next() } returns
                    TUPLE_NO_MATCH andThen
                    TUPLE_MATCH andThen
                    TUPLE_OTHER_MATCH andThen
                    null

            every { mockPredicate.evaluate(TUPLE_NO_MATCH) } returns false
            every { mockPredicate.evaluate(TUPLE_MATCH) } returns true
            every { mockPredicate.evaluate(TUPLE_OTHER_MATCH) } returns true
            val tuple1 = selection.next()
            val tuple2 = selection.next()
            val tuple3 = selection.next()

            Then("The first tuple returned should be the first match") {
                tuple1 shouldBe TUPLE_MATCH
            }

            Then("The second tuple returned should be the next match") {
                tuple2 shouldBe TUPLE_OTHER_MATCH
            }

            Then("The third call should return null (EOF)") {
                tuple3 shouldBe null
            }

            Then("The child operator should have been called four times") {
                verify(exactly = 4) { mockChildOperator.next() }
            }
        }
    }

    Given("A Selection operator where no tuples match") {
        selection = Selection(mockPredicate, mockChildOperator)

        When("next() is called once") {
            every { mockChildOperator.next() } returns TUPLE_NO_MATCH andThen TUPLE_NO_MATCH andThen null
            every { mockPredicate.evaluate(any()) } returns false
            val result = selection.next()

            Then("It should return null immediately (after consuming all child tuples)") {
                result shouldBe null
            }

            Then("The child operator should have been called three times") {
                verify(exactly = 3) { mockChildOperator.next() }
            }
        }
    }
})
