plugins {
    id("java-conventions")
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:6.0.1")
    testImplementation("io.kotest:kotest-assertions-core:6.0.5")
}

tasks.withType<Test> {
    useJUnitPlatform()
}