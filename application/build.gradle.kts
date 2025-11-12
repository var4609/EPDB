plugins {
    java
    application
}

dependencies {
    implementation(project(":engine"))
    implementation(project(":buffer"))
    implementation(project(":storage"))
}

application {
    mainClass.set("org.epdb.app.Main")
}