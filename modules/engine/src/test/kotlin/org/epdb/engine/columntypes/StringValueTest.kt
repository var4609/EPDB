package org.epdb.engine.columntypes

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.ints.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlin.test.assertFailsWith

class StringValueTest : BehaviorSpec({

    Given("A Candidate StringValue instance with some value") {
        val candidate = StringValue("K")

        When("comparing it to another StringValue") {
            Then("it should return positive value when the candidate lexographically greater") {
                val other = StringValue("A")

                candidate.comparesTo(other) shouldBeGreaterThan 0
            }

            Then("it should return 0 when the candidate lexographically equal") {
                val other = StringValue("K")

                candidate.comparesTo(other) shouldBe 0
            }

            Then("it should return negative value when the candidate lexographically smaller") {
                val other = StringValue("P")

                candidate.comparesTo(other) shouldBeLessThan 0
            }
        }

        When("comparing it with a non-String ColumnValue") {
            val incompatibleValue = IntValue(10)

            Then("it should throw a ClassCastException") {
                val exception = assertFailsWith<ClassCastException> {
                    candidate.comparesTo(incompatibleValue)
                }

                exception.message shouldNotBe null
            }
        }
    }

    Given("two StringValue objects with the same value") {
        val valueA = StringValue("TEST")
        val valueB = StringValue("TEST")

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
