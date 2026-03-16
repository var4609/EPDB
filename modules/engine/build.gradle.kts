plugins {
    id("kotlin-conventions")
}

dependencies {
    implementation(projects.buffer)
    implementation(projects.storage)
    implementation(projects.index)
    implementation(project(":commons"))
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/engine")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/engine")
}