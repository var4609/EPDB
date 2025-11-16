plugins {
    java
    jacoco
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

jacoco {
    toolVersion = "0.8.14" // Use a recent version
}