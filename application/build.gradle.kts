plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":engine"))
    testImplementation(kotlin("test"))
}

application {
    // Set the fully qualified name of your desired main class
    mainClass.set("org.epdb.app.MainKt")
}