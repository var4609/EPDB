plugins {
    application
    id("kotlin-conventions")
}

dependencies {
    implementation(project(":engine"))
    implementation(project(":buffer"))
    implementation(project(":storage"))
    implementation(project(":index"))
}

application {
    mainClass = "org.epdb.app.MainKt"
}

repositories {
    mavenCentral()
}