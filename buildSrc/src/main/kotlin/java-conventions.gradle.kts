plugins {
    java apply false
}

group = rootProject.group
version = rootProject.version

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("junit:junit:4.13")
    testImplementation("org.mockito:mockito-core:5.10.0")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
