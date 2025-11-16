plugins {
    java
    id("jacoco-report-aggregation")
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects.forEach {
    dependencies {
        jacocoAggregation(project(it.path))
    }
}

tasks.register("release") {
    group = "Release"
    description = "Cleans and runs tests across subprojects"

    dependsOn(subprojects.flatMap { project ->
        listOf(project.tasks.named("clean"), project.tasks.named("test"))
    })
}

tasks.withType<Test>().configureEach {
    maxParallelForks = Runtime.getRuntime().availableProcessors() / 2
    testLogging {
        showStandardStreams =  true
        events("failed", "skipped", "passed")
    }
}