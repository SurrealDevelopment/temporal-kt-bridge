package com.surrealdev.temporal.core.internal

import com.surrealdev.temporal.core.BuildConfig
import java.io.FileOutputStream
import java.lang.foreign.Arena
import java.lang.foreign.SymbolLookup
import java.nio.file.Files
import java.nio.file.Path

/**
 * Platform-aware native library loader for the Temporal Core bridge.
 *
 * This loader uses Java's Foreign Function & Memory (FFM) API to:
 * 1. Detect the current OS and architecture
 * 2. Extract the appropriate native library from JAR resources
 * 3. Load the library via SymbolLookup for FFM access
 */
object NativeLoader {
    private const val LIB_NAME = "kt_bridge"
    private const val SUPPORTED_CLASSIFIERS =
        "linux-x86_64-gnu, linux-aarch64-gnu, linux-x86_64-musl, linux-aarch64-musl, macos-aarch64, windows-x86_64"

    /** System property naming an explicit library to load instead of one from the classpath. */
    private const val LIBRARY_PATH_PROPERTY = "temporal.native.libraryPath"

    /** Environment-variable equivalent of [LIBRARY_PATH_PROPERTY]. */
    private const val LIBRARY_PATH_ENV = "TEMPORAL_KT_NATIVE_LIB"

    /**
     * Global arena for the native library's lifetime.
     * Using global arena ensures the library stays loaded for the JVM's lifetime.
     */
    private val arena: Arena = Arena.global()

    @Volatile
    private var symbolLookup: SymbolLookup? = null

    @Volatile
    private var libraryPath: Path? = null

    private val platform: Platform by lazy { detectPlatform() }

    /**
     * Loads an explicitly supplied library instead of one from the classpath, if configured.
     *
     * Set `-Dtemporal.native.libraryPath=/abs/path/lib...` (or the `TEMPORAL_KT_NATIVE_LIB`
     * environment variable) to run against a locally built library. This is how you test a Rust
     * change without packaging a JAR, and it is required when consuming this project as a Gradle
     * composite build, because classifier dependencies cannot be substituted that way.
     *
     * Unlike the classpath path, nothing is extracted: the file is loaded where it sits.
     *
     * @throws UnsatisfiedLinkError if a path is configured but does not point at a readable file,
     *   since silently falling back to the classpath would hide the very change being tested.
     */
    private fun loadFromOverride(): SymbolLookup? {
        val path =
            resolveOverridePath(
                System.getProperty(LIBRARY_PATH_PROPERTY) ?: System.getenv(LIBRARY_PATH_ENV),
            ) ?: return null

        System.load(path.toString())
        libraryPath = path
        return SymbolLookup.libraryLookup(path, arena).also { symbolLookup = it }
    }

    /**
     * Validates a configured override path, returning null when none is configured.
     *
     * Separate from [loadFromOverride] so it can be tested without the singleton: [load] caches
     * its lookup, so once anything has loaded the library a test calling [load] never reaches the
     * override branch at all.
     */
    internal fun resolveOverridePath(configured: String?): Path? {
        if (configured == null) return null
        val path = Path.of(configured).toAbsolutePath()
        if (!Files.isRegularFile(path)) {
            throw UnsatisfiedLinkError(
                "$LIBRARY_PATH_PROPERTY is set to '$configured', which is not a readable file. " +
                    "Point it at a built library, or unset it to use the one on the classpath.",
            )
        }
        return path
    }

    /**
     * Loads the native library and returns a SymbolLookup for accessing symbols.
     * Safe to call multiple times - returns cached lookup after first load.
     *
     * @return SymbolLookup for accessing native functions
     * @throws UnsatisfiedLinkError if the library cannot be loaded
     * @throws IllegalStateException if the platform is not supported
     */

    @Synchronized
    fun load(): SymbolLookup {
        symbolLookup?.let { return it }

        loadFromOverride()?.let { return it }

        val libFileName = platform.libFileName(LIB_NAME)
        val resourcePath = "/native/${platform.resourceDir}/$libFileName"

        val resourceStream =
            NativeLoader::class.java.getResourceAsStream(resourcePath)
                ?: throw UnsatisfiedLinkError(
                    $$"""
                    Native library not found for platform: $${platform.resourceDir}

                    Add the platform-specific dependency. Note that core-bridge is versioned as
                    <sdk-core version>-<bridge version>, so it does NOT share the version of
                    com.surrealdev.temporal:core -- use the version below verbatim.

                    Directly, for this platform:
                      runtimeOnly("com.surrealdev.temporal:core-bridge:$${BuildConfig.BRIDGE_VERSION}:$${platform.mavenClassifier}")

                    Simplest: apply the com.surrealdev.temporal Gradle plugin and use `temporal { native() }`,
                    which resolves the matching classifier for you.

                    Supported classifiers: $${SUPPORTED_CLASSIFIERS}
                    """.trimIndent(),
                )

        val tempDir = Files.createTempDirectory("temporal-core-bridge")
        val tempLib = tempDir.resolve(libFileName)

        resourceStream.use { input ->
            FileOutputStream(tempLib.toFile()).use { output ->
                input.copyTo(output)
            }
        }

        // Register cleanup on JVM shutdown
        Runtime.getRuntime().addShutdownHook(
            Thread {
                tempLib.toFile().delete()
                tempDir.toFile().delete()
            },
        )

        libraryPath = tempLib

        // Load library ahead of time so FFM bindings work ok
        System.load(tempLib.toAbsolutePath().toString())

        val lookup = SymbolLookup.libraryLookup(tempLib, arena)
        symbolLookup = lookup
        return lookup
    }

    /**
     * Check if the native library has been loaded.
     */
    fun isLoaded(): Boolean = symbolLookup != null

    /**
     * Get the path to the loaded library, or null if not loaded.
     */
    fun getLibraryPath(): Path? = libraryPath

    internal fun detectPlatform(
        osName: String = System.getProperty("os.name"),
        arch: String = System.getProperty("os.arch"),
        isMusl: Boolean = osName.contains("linux", ignoreCase = true) && detectMuslLibc(),
    ): Platform {
        val normalizedOs = osName.lowercase()
        val os =
            when {
                normalizedOs.contains("mac") || normalizedOs.contains("darwin") -> {
                    OS.MACOS
                }

                normalizedOs.contains("linux") && isMusl -> {
                    OS.LINUXMUSL
                }

                normalizedOs.contains("linux") -> {
                    OS.LINUXGNU
                }

                normalizedOs.contains("windows") -> {
                    OS.WINDOWS
                }

                else -> {
                    throw IllegalStateException("Unsupported operating system: $osName")
                }
            }

        val architecture =
            when (arch.lowercase()) {
                "aarch64", "arm64" -> Arch.AARCH64
                "amd64", "x86_64" -> Arch.X86_64
                else -> throw IllegalStateException("Unsupported architecture: $arch")
            }

        check(
            (os != OS.MACOS || architecture == Arch.AARCH64) &&
                (os != OS.WINDOWS || architecture == Arch.X86_64),
        ) {
            "Unsupported platform: $osName ($arch). Supported classifiers: $SUPPORTED_CLASSIFIERS"
        }
        return Platform(os, architecture)
    }

    /**
     * Whether this Linux runs on musl (Alpine and friends).
     *
     * Decided from the filesystem alone: the dynamic loader musl installs is `/lib/ld-musl-<arch>.so.1`,
     * and Alpine also ships `/etc/alpine-release`. The previous version forked `ldd --version`
     * on every start, which is slow, and not available in distroless images at all.
     */
    private fun detectMuslLibc(): Boolean {
        if (java.io.File("/etc/alpine-release").exists()) return true
        val lib = java.io.File("/lib")
        return lib.list()?.any { it.startsWith("ld-musl-") && it.endsWith(".so.1") } == true
    }

    internal enum class OS {
        MACOS,
        LINUXGNU,
        LINUXMUSL,
        WINDOWS,
    }

    internal enum class Arch {
        X86_64,
        AARCH64,
    }

    internal data class Platform(
        val os: OS,
        val arch: Arch,
    ) {
        /**
         * Internal resource directory path within the JAR.
         */
        val resourceDir: String
            get() =
                when (os) {
                    OS.MACOS -> "macos-${arch.name.lowercase()}"
                    OS.LINUXGNU -> "linux-${arch.name.lowercase()}-gnu"
                    OS.LINUXMUSL -> "linux-${arch.name.lowercase()}-musl"
                    OS.WINDOWS -> "windows-${arch.name.lowercase()}"
                }

        /**
         * Maven classifier for the platform-specific JAR artifact.
         * Use this when declaring the runtimeOnly dependency.
         */
        val mavenClassifier: String
            get() = resourceDir

        fun libFileName(baseName: String): String =
            when (os) {
                OS.MACOS -> "lib$baseName.dylib"
                OS.LINUXGNU, OS.LINUXMUSL -> "lib$baseName.so"
                OS.WINDOWS -> "$baseName.dll"
            }
    }
}
