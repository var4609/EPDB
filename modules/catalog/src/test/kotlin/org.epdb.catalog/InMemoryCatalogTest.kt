package org.epdb.catalog

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import org.epdb.commons.engine.Schema

class InMemoryCatalogTest : BehaviorSpec({

    Given("An empty InMemoryCatalog") {
        val tableName = "TEST_TABLE"
        val tableSchema = mockk<Schema>()
        val catalogMap = mutableMapOf<String, TableCatalog>()
        val catalog = InMemoryCatalog(catalogMap)

        When("a new TableCatalog is added") {
            catalog.setTableSchema(tableName, tableSchema)

            Then("a table named $tableName is added") {
                catalogMap[tableName]?.tableSchema shouldBe tableSchema
            }

            Then("new catalog has ID = 1") {
                catalogMap[tableName]?.tableId shouldBe 1
            }

            Then("get should return the table schema for $tableName") {
                catalog.getTableSchema(tableName) shouldBe tableSchema
            }
        }
    }

    Given("An InMemoryCatalog with existing catalogs") {
        val catalogMap = mutableMapOf<String, TableCatalog>()
        val catalog = InMemoryCatalog(catalogMap)
        val oldSchema = mockk<Schema>()
        catalog.setTableSchema("TEST_TABLE_1", oldSchema)
        catalog.setTableSchema("TEST_TABLE_2", mockk<Schema>())
        catalog.setTableSchema("TEST_TABLE_3", mockk<Schema>())

        When("setTableSchema is called for an existing catalog") {
            val newSchema = mockk<Schema>()
            catalog.setTableSchema("TEST_TABLE_1", newSchema)

            Then("Schema of the table is updated") {
                catalogMap["TEST_TABLE_1"]?.tableSchema shouldNotBe oldSchema
                catalogMap["TEST_TABLE_1"]?.tableSchema shouldBe newSchema
            }
        }

        When("a new table catalog is added") {
            catalog.setTableSchema("TEST_TABLE_4", mockk<Schema>())

            Then("tableId is set incrementally") {
                catalogMap["TEST_TABLE_4"]?.tableId shouldBe catalogMap.size
            }
        }
    }
})
