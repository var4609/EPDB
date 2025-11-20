plugins {
    application
    id("java-conventions")
}

dependencies {
    implementation(project(":engine"))
    implementation(project(":buffer"))
    implementation(project(":storage"))
}

application {
    mainClass = "org.epdb.app.Main"
}