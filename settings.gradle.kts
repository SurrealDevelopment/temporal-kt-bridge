dependencyResolutionManagement {
    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
    }
}

include(":core-common")
include(":protos")
include(":core-bridge")

rootProject.name = "temporal-kt-bridge"
