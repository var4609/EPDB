
plugins {
    java
}

dependencies {
    testImplementation(libs.junit)
    testImplementation(libs.mockito)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}