plugins {
    id("kotlin-conventions")
}

dependencies {
    implementation(project(":commons"))
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/index")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/index")
}