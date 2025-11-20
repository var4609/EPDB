plugins {
    base
    id("java-conventions")
    id("jacoco-report-aggregation")
}

dependencies {
    jacocoAggregation(project(":application"))
}

tasks.check {
    dependsOn(tasks.named<JacocoReport>("testCodeCoverageReport"))
}