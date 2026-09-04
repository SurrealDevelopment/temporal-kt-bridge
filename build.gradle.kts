plugins {
    id("org.jetbrains.dokka")
    id("org.jetbrains.dokka-javadoc")
}

dokka {
    moduleName.set("temporal-kt-bridge")

    dokkaPublications.html {
        suppressInheritedMembers.set(true)
        failOnWarning.set(false)
    }
}

dependencies {
    dokka(project(":core-bridge"))
    dokka(project(":core-common"))
}

// `protos` is ~2,100 generated files that nobody reads as docs; running Dokka over them is slow
// and memory-hungry for no benefit.
val undocumentedProjects = setOf("protos")

subprojects {
    if (name in undocumentedProjects) {
        return@subprojects
    }

    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "org.jetbrains.dokka-javadoc")

    dokka {
        moduleName.set(name)

        val moduleReadme = project.file("README.md")
        if (moduleReadme.exists()) {
            dokkaSourceSets.configureEach {
                includes.from(moduleReadme)
            }
        }

        dokkaPublications.html {
            suppressInheritedMembers.set(true)
            failOnWarning.set(false)
        }
    }
}
