plugins {
    java
    jacoco
    application
}

dependencies {
    implementation(project(":engine"))
    implementation(project(":buffer"))
    implementation(project(":storage"))
}

jacoco {
    toolVersion = "0.8.14" // Use a recent version
}

application {
    mainClass.set("org.epdb.app.Main")
}