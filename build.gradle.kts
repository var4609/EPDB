plugins {
    java
}

allprojects {
    repositories {
        mavenCentral()
    }
}

tasks.register("release") {
    group = "Release"
    description = "Runs release tasks in every subproject"
}

subprojects {
    rootProject.tasks.named("release") {
        dependsOn(tasks.named("clean"))
        dependsOn(tasks.named("build"))
    }
}