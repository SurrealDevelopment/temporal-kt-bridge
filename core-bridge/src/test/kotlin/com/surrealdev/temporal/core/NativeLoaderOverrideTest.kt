package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.NativeLoader
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFailsWith

/**
 * `-Dtemporal.native.libraryPath` is how you run against a locally built library without
 * packaging a JAR, and it is required when temporal-kt consumes this project as a Gradle
 * composite build (classifier dependencies cannot be substituted that way). If it silently did
 * nothing, the classpath library would be used instead and a Rust change under test would appear
 * to have no effect -- so the misconfigured case must be loud.
 */
class NativeLoaderOverrideTest {
    @Test
    fun `a library path pointing at nothing fails instead of falling back to the classpath`() {
        val previous = System.getProperty(PROPERTY)
        System.setProperty(PROPERTY, "/definitely/not/a/library.dylib")
        try {
            val error = assertFailsWith<UnsatisfiedLinkError> { NativeLoader.load() }
            val message = requireNotNull(error.message)
            assertContains(message, PROPERTY)
            assertContains(message, "/definitely/not/a/library.dylib")
        } finally {
            if (previous == null) System.clearProperty(PROPERTY) else System.setProperty(PROPERTY, previous)
        }
    }

    private companion object {
        const val PROPERTY = "temporal.native.libraryPath"
    }
}
