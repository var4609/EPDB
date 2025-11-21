plugins {
    id("java-conventions")
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testImplementation("io.kotest:kotest-assertions-core:6.0.5")
}