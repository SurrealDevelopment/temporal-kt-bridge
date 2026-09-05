package com.surrealdev.temporal.core

import java.io.File
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RuntimeWithoutOpenTelemetryTest {
    @Test
    fun `default runtime runs without OpenTelemetry on the classpath`() {
        // Gradle loads tests through its own classloader, outside java.class.path.
        val classpath =
            (
                generateSequence(javaClass.classLoader) { it.parent }
                    .filterIsInstance<URLClassLoader>()
                    .flatMap { it.urLs.asSequence() }
                    .map { Path.of(it.toURI()).toString() }
                    .toList() + System.getProperty("java.class.path").split(File.pathSeparator)
            ).distinct().filterNot { File(it).name.startsWith("opentelemetry-") }
        val output = Files.createTempFile("temporal-without-otel", ".log")
        val command =
            buildList {
                add(Path.of(System.getProperty("java.home"), "bin", "java").toString())
                add("--enable-native-access=ALL-UNNAMED")
                System.getProperty("temporal.native.libraryPath")?.let { add("-Dtemporal.native.libraryPath=$it") }
                addAll(
                    listOf(
                        "-cp",
                        classpath.joinToString(File.pathSeparator),
                        RuntimeWithoutOpenTelemetryProbe::class.java.name,
                    ),
                )
            }
        val process = ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start()
        try {
            assertTrue(
                process.waitFor(30, TimeUnit.SECONDS),
                "Runtime subprocess timed out: ${Files.readString(output)}",
            )
            assertEquals(0, process.exitValue(), Files.readString(output))
        } finally {
            process.destroyForcibly()
            Files.deleteIfExists(output)
        }
    }
}

object RuntimeWithoutOpenTelemetryProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        check(
            runCatching {
                Class.forName(
                    "io.opentelemetry.api.metrics.Meter",
                )
            }.exceptionOrNull() is ClassNotFoundException,
        )
        TemporalRuntime.create().use { Thread.sleep(150) }
    }
}
