package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.NativeLoader
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * `-Dtemporal.native.libraryPath` is how you run against a locally built library without
 * packaging a JAR, and it is required when temporal-kt consumes this project as a Gradle
 * composite build (classifier dependencies cannot be substituted that way). If a misconfigured
 * path silently fell back to the classpath, the library under test would be ignored and a Rust
 * change would appear to have no effect.
 *
 * These exercise the validation directly rather than going through `NativeLoader.load()`, which
 * caches its lookup: whether `load()` reaches the override branch depends on whether another
 * test loaded the library first, which is not something a test should depend on.
 */
class NativeLoaderOverrideTest {
    @Test
    fun `no configured path means no override`() {
        assertNull(NativeLoader.resolveOverridePath(null))
    }

    @Test
    fun `a path pointing at nothing fails instead of falling back to the classpath`() {
        val error =
            assertFailsWith<UnsatisfiedLinkError> {
                NativeLoader.resolveOverridePath("/definitely/not/a/library.dylib")
            }
        val message = requireNotNull(error.message)
        assertContains(message, "temporal.native.libraryPath")
        assertContains(message, "/definitely/not/a/library.dylib")
    }

    @Test
    fun `a directory is rejected, not treated as a library`() {
        assertFailsWith<UnsatisfiedLinkError> {
            NativeLoader.resolveOverridePath(System.getProperty("java.io.tmpdir"))
        }
    }

    @Test
    fun `an existing file resolves to an absolute path`() {
        val file = createTempFile("temporal-native-override", ".so")
        try {
            assertEquals(file.toAbsolutePath(), NativeLoader.resolveOverridePath(file.toString()))
        } finally {
            file.deleteIfExists()
        }
    }
}
