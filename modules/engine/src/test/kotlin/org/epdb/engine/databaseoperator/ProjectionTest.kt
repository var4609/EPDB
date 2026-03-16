package org.epdb.engine.databaseoperator

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.epdb.engine.columntypes.IntValue
import org.epdb.engine.columntypes.StringValue
import org.epdb.engine.dto.Tuple

class ProjectionTest : BehaviorSpec({

    val ID_INDEX = 0
    val NAME_INDEX = 1
    val AGE_INDEX = 2
    val FULL_TUPLE = Tuple(
        values = listOf(
            IntValue(1),        // Index 0: ID
            StringValue("Alice"), // Index 1: Name
            IntValue(30)         // Index 2: Age
        )
    )
    val EMPTY_TUPLE = Tuple(emptyList())

    val mockChildOperator : Operator = mockk(relaxed = true)
    lateinit var projection: Projection

    beforeContainer {
        clearAllMocks()
        every { mockChildOperator.open() } just Runs
        every { mockChildOperator.close() } just Runs
    }

    Given("A Projection operator") {
        val allColumns = setOf(ID_INDEX, NAME_INDEX, AGE_INDEX)

        When("open() is called") {
            projection = Projection(mockChildOperator, allColumns)
            projection.open()

            Then("It should open the child operator") {
                verify(exactly = 1) { mockChildOperator.open() }
            }
        }

        When("close() is called") {
            projection = Projection(mockChildOperator, allColumns)
            projection.close()

            Then("It should close the child operator") {
                verify(exactly = 1) { mockChildOperator.close() }
            }
        }
    }

    Given("A Projection operator configured to select all columns (ID, Name, Age)") {
        val projectionColumns = setOf(ID_INDEX, NAME_INDEX, AGE_INDEX)
        projection = Projection(mockChildOperator, projectionColumns)

        When("next() is called") {
            every { mockChildOperator.next() } returns FULL_TUPLE andThen null
            val result = projection.next()

            Then("The resulting tuple should contain all original values in order") {
                result shouldBe FULL_TUPLE
            }
        }
    }

    Given("A Projection operator configured to select only two columns (Name, Age)") {
        val projectionColumns = setOf(NAME_INDEX, AGE_INDEX) // Indices 1, 2
        projection = Projection(mockChildOperator, projectionColumns)

        When("next() is called") {
            every { mockChildOperator.next() } returns FULL_TUPLE andThen null
            val result = projection.next()

            Then("The resulting tuple should have exactly two values") {
                result?.values?.size shouldBe 2
            }

            Then("The resulting values should be Name and Age") {
                result?.values shouldContainExactly listOf(
                    StringValue("Alice"),
                    IntValue(30)
                )
            }
        }
    }

    Given("A Projection operator configured to select columns in a different order (Age, ID)") {
        val projectionColumns = setOf(AGE_INDEX, ID_INDEX)
        projection = Projection(mockChildOperator, projectionColumns)

        When("next() is called") {
            every { mockChildOperator.next() } returns FULL_TUPLE andThen null
            val result = projection.next()

            Then("The resulting tuple values should be ID then Age (based on input index order)") {
                result?.values shouldContainExactly listOf(
                    IntValue(1),      // ID (Index 0)
                    IntValue(30)      // Age (Index 2)
                )
            }
        }
    }

    Given("A Projection operator where the child reaches EOF") {
        val projectionColumns = setOf(ID_INDEX)
        projection = Projection(mockChildOperator, projectionColumns)

        When("next() is called") {
            every { mockChildOperator.next() } returns null
            val result = projection.next()

            Then("It should return null") {
                result shouldBe null
            }
        }
    }

    Given("A Projection operator configured to select an empty set of columns") {
        val projectionColumns = emptySet<Int>()
        projection = Projection(mockChildOperator, projectionColumns)

        When("next() is called") {
            every { mockChildOperator.next() } returns FULL_TUPLE andThen null
            val result = projection.next()

            Then("It should return a tuple with an empty list of values") {
                result shouldBe EMPTY_TUPLE
            }
        }
    }
})