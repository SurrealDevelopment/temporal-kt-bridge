import org.gradle.internal.os.OperatingSystem

plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("buildsrc.convention.maven-publish")
    id("com.github.gmazzo.buildconfig")
    alias(libs.plugins.protobuf)
}

// Versioned as `<sdkCoreVersion>-<temporal-kt version>` (e.g. 0.6.0-0.1.11): this artifact's
// content is determined by a Temporal SDK-Core release as much as by temporal-kt, so the
// coordinate says which Core it speaks to. See gradle.properties.
val sdkCoreVersion: String by project
val bridgeAbi: String by project
version = "$sdkCoreVersion-${rootProject.version}"

// Platform classifier to internal resource directory mapping
data class NativePlatform(
    val classifier: String,
    val resourceDir: String,
)

val nativePlatforms =
    listOf(
        NativePlatform("linux-x86_64-gnu", "linux-x86_64-gnu"),
        NativePlatform("linux-aarch64-gnu", "linux-aarch64-gnu"),
        NativePlatform("linux-x86_64-musl", "linux-x86_64-musl"),
        NativePlatform("linux-aarch64-musl", "linux-aarch64-musl"),
        NativePlatform("macos-aarch64", "macos-aarch64"),
        NativePlatform("windows-x86_64", "windows-x86_64"),
    )

dependencies {
    api(project(":core-common"))
    // Proto types appear in this module's own signatures (worker poll/complete traffic in
    // `coresdk.*`, TemporalTestServer returns io.temporal.api.testservice.v1.*), so this is api.
    api(project(":protos"))
    implementation(libs.kotlinxCoroutines)
    implementation(libs.slf4jApi)
    compileOnly(libs.opentelemetryApi)

    testImplementation(kotlin("test"))
    testImplementation(libs.opentelemetrySdkTesting)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
    }
}

sourceSets.main {
    proto { setSrcDirs(listOf("rust/kt-bridge/proto")) }
}

// Detect current platform
val os: OperatingSystem = OperatingSystem.current()
val arch: String = System.getProperty("os.arch").lowercase()
val linuxLibc = if (File("/lib").listFiles()?.any { it.name.startsWith("ld-musl-") } == true) "musl" else "gnu"

val nativePlatform: String =
    when {
        os.isMacOsX && arch == "aarch64" -> "macos-aarch64"
        os.isLinux && arch == "aarch64" -> "linux-aarch64-$linuxLibc"
        os.isLinux && arch in setOf("amd64", "x86_64") -> "linux-x86_64-$linuxLibc"
        os.isWindows && arch in setOf("amd64", "x86_64") -> "windows-x86_64"
        else -> throw GradleException("Unsupported platform: ${os.name} / $arch")
    }

val libPrefix: String = if (os.isWindows) "" else "lib"
val libExtension: String =
    when {
        os.isMacOsX -> "dylib"
        os.isLinux -> "so"
        os.isWindows -> "dll"
        else -> throw GradleException("Unsupported platform")
    }

val nativeLibName = "kt_bridge"

// Output directory for native libraries (in build folder, not src)
val nativeLibsDir = layout.buildDirectory.dir("native-libs")

// Set -PskipNativeBuild=true to skip native library building (used in CI publish job)
val skipNativeBuild = project.findProperty("skipNativeBuild")?.toString()?.toBoolean() ?: false

/**
 * Registers `cargoBuild<Name>` + `copyNativeLib<Name>` for one target.
 *
 * `target` is null for the host build, which cargo writes to `target/release` rather than
 * `target/<triple>/release`.
 *
 * `zig` selects `cargo zigbuild`, which cross-compiles every Linux flavour from one runner. For
 * gnu targets `glibc` pins the floor the library links against: the natives used to be built on
 * whatever glibc the runner had (2.39 on ubuntu-24.04), which fails with an obscure
 * UnsatisfiedLinkError on Debian 12 (2.36), Ubuntu 22.04 (2.35) or RHEL 9 (2.34). 2.17 is Rust's
 * own minimum and covers RHEL 7 onwards. musl targets take no floor and are what Alpine loads.
 *
 * kt-bridge depends on sdk-core from crates.io, so cargo resolves and caches it like any other
 * dependency -- there is no vendored source tree to declare as an input. The inputs below are
 * this crate and its lockfile, which is the whole of what we own.
 */
fun registerNativeBuild(
    name: String,
    resourceDir: String,
    target: String?,
    libFile: String,
    zig: Boolean = false,
    glibc: String? = null,
): TaskProvider<Sync> {
    // zigbuild's `<triple>.<glibc>` spelling is a request, not a directory: output still lands in
    // target/<triple>/release.
    val outDir = if (target == null) "release" else "$target/release"
    val builtLib = "rust/kt-bridge/target/$outDir/$libFile"
    val requested = if (target != null && glibc != null) "$target.$glibc" else target

    val build =
        tasks.register<Exec>("cargoBuild$name") {
            description = "Build the kt-bridge native library for $resourceDir"
            group = "build"
            workingDir = file("rust/kt-bridge")
            commandLine(
                buildList {
                    addAll(listOf("cargo", if (zig) "zigbuild" else "build", "--release", "--locked"))
                    if (requested != null) addAll(listOf("--target", requested))
                },
            )

            inputs.files(
                fileTree("rust/kt-bridge") {
                    include("Cargo.toml", "Cargo.lock", "build.rs", "rust-toolchain.toml")
                    include("src/**/*.rs", "proto/**/*.proto", ".cargo/**")
                },
            )
            outputs.file(builtLib)
            // The guard lives HERE, on the cargo task, not on the copy. A `Copy` whose source is
            // missing is not merely a no-op: Gradle marks it NO-SOURCE and skips it entirely,
            // doLast included -- which is how a musl build that emitted an rlib instead of a
            // cdylib went green with nothing to ship. An Exec task always runs its actions.
            // `builtFile` is a RegularFile hoisted out of the action so the configuration cache
            // does not have to serialise the build script.
            val builtFile = layout.projectDirectory.file(builtLib)
            doLast {
                if (!builtFile.asFile.isFile) {
                    throw GradleException(
                        "cargo reported success but produced no library at $builtLib -- " +
                            "check for 'dropping unsupported crate type' in the output",
                    )
                }
            }
        }

    // Sync, not Copy: it removes anything else in the destination, so a library from a previous
    // build (or a previous bridge) cannot ride along into the classifier JAR next to the new one.
    return tasks.register<Sync>("copyNativeLib$name") {
        description = "Place the $resourceDir native library into the build directory"
        group = "build"
        dependsOn(build)
        from(builtLib)
        into(nativeLibsDir.map { it.dir("native/$resourceDir") })
    }
}

// Host build, used by this build's own tests and by `nativeRuntime` consumers.
val copyNativeLib =
    registerNativeBuild(
        name = "",
        resourceDir = nativePlatform,
        target = null,
        libFile = "$libPrefix$nativeLibName.$libExtension",
    )

// Cross/CI builds, one per shipped classifier. Linux is cross-compiled with zig from one runner;
// macOS and Windows build natively on their own runners.
val glibcFloor = "2.17"
val copyNativeLibLinuxx8664 =
    registerNativeBuild(
        "Linuxx8664",
        "linux-x86_64-gnu",
        "x86_64-unknown-linux-gnu",
        "lib$nativeLibName.so",
        zig = true,
        glibc = glibcFloor,
    )
val copyNativeLibLinuxAarch64 =
    registerNativeBuild(
        "LinuxAarch64",
        "linux-aarch64-gnu",
        "aarch64-unknown-linux-gnu",
        "lib$nativeLibName.so",
        zig = true,
        glibc = glibcFloor,
    )
val copyNativeLibLinuxx8664Musl =
    registerNativeBuild(
        "Linuxx8664Musl",
        "linux-x86_64-musl",
        "x86_64-unknown-linux-musl",
        "lib$nativeLibName.so",
        zig = true,
    )
val copyNativeLibLinuxAarch64Musl =
    registerNativeBuild(
        "LinuxAarch64Musl",
        "linux-aarch64-musl",
        "aarch64-unknown-linux-musl",
        "lib$nativeLibName.so",
        zig = true,
    )
val copyNativeLibWindowsx8664 =
    registerNativeBuild("Windowsx8664", "windows-x86_64", "x86_64-pc-windows-msvc", "$nativeLibName.dll")
val copyNativeLibMacosAarch64 =
    registerNativeBuild("MacosAarch64", "macos-aarch64", "aarch64-apple-darwin", "lib$nativeLibName.dylib")

val copyAllNativeLibs by tasks.registering {
    description = "Copy all native libraries to build directory"
    group = "build"
    dependsOn(
        copyNativeLibLinuxx8664,
        copyNativeLibLinuxAarch64,
        copyNativeLibLinuxx8664Musl,
        copyNativeLibLinuxAarch64Musl,
        copyNativeLibWindowsx8664,
        copyNativeLibMacosAarch64,
    )
}

// Platform-specific aggregator tasks for CI matrix builds
val copyLinuxNativeLibs by tasks.registering {
    description = "Copy all four Linux native libraries (gnu + musl, x86_64 + aarch64), cross-built with zig"
    group = "build"
    dependsOn(
        copyNativeLibLinuxx8664,
        copyNativeLibLinuxAarch64,
        copyNativeLibLinuxx8664Musl,
        copyNativeLibLinuxAarch64Musl,
    )
}

val copyMacosAarch64NativeLib by tasks.registering {
    description = "Copy macOS ARM64 native library (for ARM Mac CI runner)"
    group = "build"
    dependsOn(copyNativeLibMacosAarch64)
}

val copyWindowsNativeLib by tasks.registering {
    description = "Copy Windows native library (for Windows CI runner)"
    group = "build"
    dependsOn(copyNativeLibWindowsx8664)
}

val nativeCopies =
    mapOf(
        "linux-x86_64-gnu" to copyNativeLibLinuxx8664,
        "linux-aarch64-gnu" to copyNativeLibLinuxAarch64,
        "linux-x86_64-musl" to copyNativeLibLinuxx8664Musl,
        "linux-aarch64-musl" to copyNativeLibLinuxAarch64Musl,
        "macos-aarch64" to copyNativeLibMacosAarch64,
        "windows-x86_64" to copyNativeLibWindowsx8664,
    )

// Create platform-specific classifier JARs containing only the native library
nativePlatforms.forEach { platform ->
    val taskName = "${platform.classifier.replace("-", "").replace("_", "")}NativeJar"
    tasks.register<Jar>(taskName) {
        description = "Create classifier JAR with native library for ${platform.classifier}"
        group = "build"
        archiveClassifier.set(platform.classifier)
        if (!skipNativeBuild) {
            dependsOn(
                if (platform.classifier ==
                    nativePlatform
                ) {
                    copyNativeLib
                } else {
                    nativeCopies.getValue(platform.classifier)
                },
            )
        }
        val nativeDir = nativeLibsDir.map { it.dir("native/${platform.resourceDir}") }
        val libraryName =
            when {
                platform.classifier.startsWith("windows") -> "kt_bridge.dll"
                platform.classifier.startsWith("macos") -> "libkt_bridge.dylib"
                else -> "libkt_bridge.so"
            }
        doFirst {
            check(
                nativeDir
                    .get()
                    .file(libraryName)
                    .asFile.isFile,
            ) {
                "Missing $libraryName for ${platform.classifier}; build or download the native before publishing"
            }
        }
        from(nativeLibsDir.map { it.dir("native/${platform.resourceDir}") }) {
            into("native/${platform.resourceDir}")
        }
    }
}

// Host-platform native library, exposed to other modules in this build.
//
// A consumer in this build declares
// `testRuntimeOnly(project(path = ":core-bridge", configuration = "nativeRuntime"))` to get the
// native as a single JAR on the classpath -- the same shape it has when resolved from Maven as a
// classifier artifact, so local development and downstream exercise one path. temporal-kt uses
// exactly this via its `temporal-native-test` convention plugin, against the published artifact.
//
// When -PskipNativeBuild=true the JAR is packaged from whatever is already in build/native-libs
// (CI downloads prebuilt libraries there), so cargo is not invoked.
val hostNativeJar by tasks.registering(Jar::class) {
    description = "JAR containing the host platform's native library, for consumers inside this build"
    group = "build"
    archiveClassifier.set("native-host")
    val hostLibrary = nativeLibsDir.map { it.file("native/$nativePlatform/$libPrefix$nativeLibName.$libExtension") }
    doFirst { check(hostLibrary.get().asFile.isFile) { "Host native library is missing: ${hostLibrary.get()}" } }
    if (!skipNativeBuild) {
        dependsOn(copyNativeLib)
    }
    from(nativeLibsDir.map { it.dir("native/$nativePlatform") }) {
        into("native/$nativePlatform")
    }
}

val nativeRuntime =
    configurations.consumable("nativeRuntime") {
        description = "Host-platform native library, as a JAR, for other modules in this build"
    }

artifacts.add(nativeRuntime.name, hostNativeJar)

// For local development/testing, include current platform's native lib in test resources
tasks.named<ProcessResources>("processTestResources") {
    if (!skipNativeBuild) {
        dependsOn(copyNativeLib)
    }
    from(nativeLibsDir) {
        into("")
    }
}

// Clean task for Rust artifacts
tasks.register<Delete>("cargoClean") {
    description = "Clean Rust build artifacts"
    group = "build"
    delete("rust/kt-bridge/target")
}

tasks.named("clean") {
    dependsOn("cargoClean")
}

// Enable native access for FFM API to suppress warnings
tasks.withType<Test> {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

// Generate BuildConfig with version constants
val temporalCliVersion: String by project

buildConfig {
    packageName("com.surrealdev.temporal.core")
    documentation.set("Build-time configuration constants.")

    buildConfigField("TEMPORAL_CLI_VERSION", temporalCliVersion)

    // The temporal-kt SDK version, NOT this module's composite version. This is reported to the
    // Temporal server as the client version (see TemporalCoreClient.clientVersion), where
    // "0.6.0-0.1.11" would wrongly read as an SDK 0.6.0. rootProject.version is the SDK version.
    buildConfigField("SDK_VERSION", rootProject.version.toString())

    // This module's own coordinate, `<sdkCoreVersion>-<version>`, for diagnostics.
    buildConfigField("BRIDGE_VERSION", project.version.toString())
    buildConfigField("SDK_CORE_VERSION", sdkCoreVersion)

    // Compatibility number for the seam `core` consumes. See gradle.properties.
    buildConfigField("ABI_VERSION", bridgeAbi.toInt())
}

mavenPublishing {
    coordinates(artifactId = "core-bridge")

    pom {
        name.set("Temporal KT Core Bridge")
        description.set("Kotlin FFM Bridge to Temporal Core SDK")
    }
}

// Configure publishing to include platform-specific classifier JARs
afterEvaluate {
    publishing {
        publications {
            named<MavenPublication>("maven") {
                nativePlatforms.forEach { platform ->
                    val taskName = "${platform.classifier.replace("-", "").replace("_", "")}NativeJar"
                    artifact(tasks.named(taskName)) {
                        classifier = platform.classifier
                    }
                }
            }
        }
    }
}
