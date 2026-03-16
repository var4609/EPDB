package org.epdb.index

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainInOrder
import io.kotest.matchers.shouldBe
import org.epdb.index.dto.PagePointer
import org.epdb.index.manager.InMemoryIndexManager

class InMemoryIndexManagerTest : BehaviorSpec({
    Given("An empty InMemoryIndexManager") {
        val indexManager = InMemoryIndexManager()
        val key = "TEST_KEY"
        val pagePointer1 = PagePointer(0L, 10)
        val pagePointer2 = PagePointer(1L, 20)

        When("an entry is added") {
            indexManager.addEntry(
                key = key,
                pageId = pagePointer1.pageId,
                slotIndex = pagePointer1.slotIndex
            )

            Then("lookupEntry should return the added pointer") {
                indexManager.lookupEntry(key) shouldContainExactly listOf(pagePointer1)
            }
        }

        When("multiple entries are added for the same key") {
            indexManager.addEntry(
                key = key,
                pageId = pagePointer2.pageId,
                slotIndex = pagePointer2.slotIndex
            )

            Then("lookupEntry should return all entries in order") {
                indexManager.lookupEntry(key) shouldContainInOrder listOf(pagePointer1, pagePointer2)
            }
        }

        When("lookupEntry is called for a non existent key") {
            val result = indexManager.lookupEntry("NON_EXISTENT_KEY")

            Then("it should return an empty list") {
                result shouldBe emptyList()
            }
        }
    }

    Given("An InMemoryIndexManager with existing entries") {

        val existingStore = mutableMapOf<Any, MutableList<PagePointer>>()
        val key = "TEST_KEY"
        val pagePointer1 = PagePointer(0L, 10)
        val pagePointer2 = PagePointer(1L, 20)
        existingStore[key] = mutableListOf(pagePointer1, pagePointer2)
        val indexManager = InMemoryIndexManager(existingStore)

        When("an existing entry is removed") {
            val result = indexManager.removeEntry(
                key = key,
                pageId = pagePointer1.pageId,
                slotIndex = pagePointer1.slotIndex
            )

            Then("removeEntry should return true") {
                result shouldBe true
            }

            Then("lookupEntry should only contain the remaining pointer") {
                indexManager.lookupEntry(key) shouldContainExactly listOf(pagePointer2)
            }
        }

        When("a non existent page pointer is attempted to be removed") {
            val nonExistentPointer = PagePointer(99L, 999)
            val removalResult = indexManager.removeEntry(
                key = key,
                pageId = nonExistentPointer.pageId,
                slotIndex = nonExistentPointer.slotIndex
            )

            Then("removeEntry should return false") {
                removalResult shouldBe false
            }

            Then("lookupEntry should still return all original pointers") {
                indexManager.lookupEntry(key) shouldContainExactly listOf(pagePointer2)
            }
        }

        When("an entry for a non existent key is attempted to be removed") {
            val removalResult = indexManager.removeEntry(
                key = "NON_EXISTENT_KEY",
                pageId = pagePointer1.pageId,
                slotIndex = pagePointer1.slotIndex
            )

            Then("removeEntry should return false") {
                removalResult shouldBe false
            }
        }

    }
})