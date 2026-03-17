plugins {
    id("kotlin-conventions")
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/catalog")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/catalog")
}