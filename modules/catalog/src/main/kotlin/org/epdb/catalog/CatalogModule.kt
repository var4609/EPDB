package org.epdb.catalog

object CatalogModule {

    private val tableCatalog by lazy {  mutableMapOf<String, TableCatalog>() }

    val catalog: Catalog by lazy {
        InMemoryCatalog(tableCatalog)
    }
}