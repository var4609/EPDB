plugins {
    id("kotlin-conventions")
}

dependencies {
    implementation(project(":storage"))
    implementation(project(":commons"))
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/buffer")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/buffer")
}
