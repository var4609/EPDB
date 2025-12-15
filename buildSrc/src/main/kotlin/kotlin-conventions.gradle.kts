group = rootProject.group
version = rootProject.version

plugins {
    kotlin("jvm")
    alias(libs.plugins.kover) apply false
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("junit:junit:4.13")
    testImplementation("org.mockito:mockito-core:5.10.0")
    testImplementation("io.mockk:mockk:1.14.6")
    testImplementation("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:6.0.1")
    testImplementation("io.kotest:kotest-assertions-core:6.0.5")
}

tasks.withType<Test> {
    useJUnitPlatform()
}