// The code in this file is a convention plugin - a Gradle mechanism for sharing reusable build logic.
// `buildSrc` is a Gradle-recognized directory and every plugin there will be easily available in the rest of the build.
package buildsrc.convention

import dev.detekt.gradle.Detekt
import dev.detekt.gradle.extensions.DetektExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jlleitschuh.gradle.ktlint.KtlintExtension

plugins {
    // Apply the Kotlin JVM plugin to add support for Kotlin in JVM projects.
    kotlin("jvm")
    // ktlint for code formatting
    id("org.jlleitschuh.gradle.ktlint")
    id("dev.detekt")
    // Dokka for documentation generation
    id("org.jetbrains.dokka")
}

configure<DetektExtension> {
    config.setFrom(rootProject.file("config/detekt.yml"))
    buildUponDefaultConfig.set(false)
}

// The plain task has no type resolution; make both detekt and check run typed analysis.
tasks.named<Detekt>("detekt") {
    enabled = false
    dependsOn("detektMain", "detektTest")
}

tasks.withType<Detekt>().configureEach {
    // Source-set roots strip build/generated from relative paths, so use the actual file path.
    val generatedDir = layout.buildDirectory.get().asFile
    exclude { it.file.startsWith(generatedDir) }
}

configure<KtlintExtension> {
    version.set("1.5.0")
    filter {
        exclude("**/generated/**")
        exclude("**/generated-sources/**")
    }
}

kotlin {
    jvmToolchain(25)
}

// Enable native access for FFM (Foreign Function & Memory) API
val nativeAccessArgs = listOf("--enable-native-access=ALL-UNNAMED")

tasks.withType<Test>().configureEach {
    // Configure all test Gradle tasks to use JUnitPlatform.
    useJUnitPlatform {
        providers.gradleProperty("excludeTags").orNull?.let {
            excludeTags(*it.split(",").toTypedArray())
        }
    }

    // Enable native access for FFM
    jvmArgs(nativeAccessArgs)

    // Log information about all test results, not only the failed ones.
    testLogging {
        events(
            TestLogEvent.FAILED,
            TestLogEvent.PASSED,
            TestLogEvent.SKIPPED,
        )
        exceptionFormat = TestExceptionFormat.FULL
        showStackTraces = true
    }
}

tasks.withType<JavaExec>().configureEach {
    // Enable native access for FFM
    jvmArgs(nativeAccessArgs)
}

// NOTE: the native library is deliberately NOT wired in here.
//
// This plugin is applied to every module, so wiring the native into test resources here would
// make every module's `processTestResources` depend on a full Rust build. `core-bridge` -- the
// only module that executes native code -- wires its own test resources instead.
