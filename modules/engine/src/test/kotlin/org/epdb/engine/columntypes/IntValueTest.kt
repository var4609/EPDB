package org.epdb.engine.columntypes

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlin.test.assertFailsWith

class IntValueTest : BehaviorSpec({

    Given("A Candidate IntValue instance with some value") {
        val candidate = ColumnValue.IntValue(10)

        When("comparing it to another IntValue") {
            Then("it should return 1 when the candidate is greater") {
                val other = ColumnValue.IntValue(5)

                candidate.compareTo(other) shouldBe 1
            }

            Then("it should return 0 when the candidate is equal") {
                val other = ColumnValue.IntValue(10)

                candidate.compareTo(other) shouldBe 0
            }

            Then("it should return -1 when the candidate is smaller") {
                val other = ColumnValue.IntValue(15)

                candidate.compareTo(other) shouldBe -1
            }
        }

        When("comparing it with a non-Int ColumnValue") {
            val incompatibleValue = ColumnValue.StringValue("test")

            Then("it should throw a ClassCastException") {
                val exception = assertFailsWith<ClassCastException> {
                    candidate.compareTo(incompatibleValue)
                }

                exception.message shouldNotBe null
            }
        }
    }

    Given("two IntValue objects with the same value") {
        val valueA = ColumnValue.IntValue(42)
        val valueB = ColumnValue.IntValue(42)

        When("they are compared for equality") {

            Then("they should be equal") {
                valueA shouldBe valueB
            }

            Then("they should have the same hash code") {
                valueA.hashCode() shouldBe valueB.hashCode()
            }
        }
    }
})