package org.epdb

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.TaskProvider

class ReleaseConventionsPlugin: Plugin<Project> {

    override fun apply(project: Project) {
        project.tasks.register("release") {
            group = "release"
            description = "Runs the full build lifecycle (clean, build, and test) for this project."

            val cleanTask: TaskProvider<*> = project.tasks.named("clean")
            val testTask: TaskProvider<*> = project.tasks.named("test")

            dependsOn(cleanTask, testTask)
        }
    }
}