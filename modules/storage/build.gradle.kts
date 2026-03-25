plugins {
    id("kotlin-conventions")
}

dependencies {
    implementation(project(":commons"))
    implementation(project(":catalog"))
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/storage")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/storage")
}