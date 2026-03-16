plugins {
    id("kotlin-conventions")
}

dependencies {
    implementation(project(":application"))
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/integration")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/integration")
}