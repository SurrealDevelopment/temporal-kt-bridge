package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.NativeLoader
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Covers extracting and loading the packaged native library from classpath resources.
 *
 * This is the path every consumer takes -- the classifier JAR on the classpath, no
 * `temporal.native.libraryPath` override -- so it is what proves the packaging is right. Runtime
 * and worker behaviour on top of the loaded library is covered by the Kt* tests.
 */
class NativeLoaderTest {
    @Test
    fun `native library loads successfully`() {
        NativeLoader.load()
        assertTrue(NativeLoader.isLoaded(), "Native library should be loaded")
    }

    @Test
    fun `multiple load calls are safe`() {
        NativeLoader.load()
        NativeLoader.load()
        NativeLoader.load()
        assertTrue(NativeLoader.isLoaded())
    }
}
