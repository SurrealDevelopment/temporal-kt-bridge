package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.NativeLoader
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class NativePlatformTest {
    @Test
    fun `supported platforms resolve exactly the shipped classifier names`() {
        val expected =
            mapOf(
                Triple("Linux", "amd64", false) to "linux-x86_64-gnu",
                Triple("Linux", "aarch64", false) to "linux-aarch64-gnu",
                Triple("Linux", "x86_64", true) to "linux-x86_64-musl",
                Triple("Linux", "arm64", true) to "linux-aarch64-musl",
                Triple("Mac OS X", "aarch64", false) to "macos-aarch64",
                Triple("Darwin", "arm64", false) to "macos-aarch64",
                Triple("Windows 11", "amd64", false) to "windows-x86_64",
            )
        expected.forEach { (input, classifier) ->
            val platform = NativeLoader.detectPlatform(input.first, input.second, input.third)
            assertEquals(classifier, platform.mavenClassifier)
            assertEquals(classifier, platform.resourceDir)
        }
    }

    @Test
    fun `unsupported operating systems and architectures fail before resource lookup`() {
        listOf("Mac OS X" to "x86_64", "Windows 11" to "aarch64", "FreeBSD" to "amd64", "Linux" to "riscv64")
            .forEach { (os, arch) ->
                val error =
                    assertFailsWith<IllegalStateException> {
                        NativeLoader.detectPlatform(os, arch, false)
                    }
                assertContains(error.message.orEmpty(), "Unsupported")
            }
    }
}
