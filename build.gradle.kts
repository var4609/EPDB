plugins {
    id("kotlin-conventions")
}

repositories {
    mavenCentral()
}

dependencies {
    kover(projects.application)
    kover(project(":buffer"))
    kover(project(":engine"))
    kover(project(":index"))
    kover(project(":storage"))
}