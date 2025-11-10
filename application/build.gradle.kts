plugins {
    java
    application
}

dependencies {
    implementation(project(":engine"))
}

application {
    mainClass.set("org.epdb.app.Main")
}