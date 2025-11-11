plugins {
    java
    application
}

dependencies {
    implementation(project(":storage"))
    
    testImplementation(libs.junit)
    testImplementation(libs.mockito)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}