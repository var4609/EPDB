plugins {
    id("kotlin-conventions")
}

dependencies {
    implementation(projects.buffer)
    implementation(projects.storage)
    implementation(projects.index)
    implementation(project(":commons"))
}