plugins {
    application
    id("kotlin-conventions")
}

dependencies {
    implementation(project(":engine"))
    implementation(project(":buffer"))
    implementation(project(":storage"))
    implementation(project(":index"))
    implementation(project(":catalog"))
    implementation(project(":commons"))
}

application {
    mainClass = "org.epdb.app.MainKt"
}

repositories {
    mavenCentral()
}

sourceSets.main {
    kotlin.srcDirs("src/main/kotlin/org/epdb/app")
}

sourceSets.test {
    kotlin.srcDirs("src/test/kotlin/org/epdb/app")
}