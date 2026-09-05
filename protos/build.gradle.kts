import buildsrc.protoManifest
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import java.net.URI

plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("buildsrc.convention.maven-publish")
    alias(libs.plugins.protobuf)
}

// Versioned as `<protosSdkCoreVersion>-<temporal-kt version>` (e.g. 0.8.0-0.1.11): this
// artifact IS a Temporal SDK-Core release's schema, so the coordinate says which one. It uses
// protosSdkCoreVersion rather than sdkCoreVersion because the schema can run ahead of the C
// bridge. See gradle.properties and PINNED.toml.
val protosSdkCoreVersion: String by project
version = "$protosSdkCoreVersion-${rootProject.version}"

// The generated protobuf classes for Temporal's API and SDK-Core's own `coresdk.*` messages.
//
// These live in their own module because they are the single largest cost in a clean build --
// 70 .proto files expand to roughly 2,100 generated sources (~1,340 .java + ~760 .kt) -- and they
// change only when Temporal's API or SDK-Core's local protos change, which is far less often than
// anything else in this repo. Keeping them here means a change to the bridge or to `core` no
// longer regenerates and recompiles all of them.
//
// The .proto sources are vendored from the temporalio-protos crate. Since the bridge moved to its own
// repository these are vendored into `protos/src/main/proto` instead, so that building the JVM
// artifacts needs neither cargo nor a submodule.
dependencies {
    // Both are part of this module's ABI: message types come from protobuf-java, and the
    // generated Kotlin DSL builders (e.g. `coresdk.activityTaskCompletion { ... }`) come from
    // protobuf-kotlin. Consumers cannot compile against the generated code without them.
    api(libs.protobufJava)
    api(libs.protobufKotlin)

    testImplementation(kotlin("test"))
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
    }
    generateProtoTasks {
        all().forEach { task ->
            task.builtins {
                create("kotlin")
            }
        }
    }
}

// The .proto sources are vendored under src/main/proto (see PINNED.toml for provenance) rather
// than read out of a checkout of SDK-Core. That keeps this module buildable with neither cargo
// nor a submodule checkout, keeps Gradle's build cache relocatable -- task inputs would otherwise
// contain absolute ~/.cargo/registry paths -- and makes a proto bump a reviewable diff instead of
// an opaque submodule pointer move.
val vendoredProtos = layout.projectDirectory.dir("src/main/proto")

sourceSets {
    main {
        proto {
            // setSrcDirs, not srcDir: the protobuf plugin adds src/main/proto itself, and the
            // vendored trees live underneath it. Adding to the default would make protoc see
            // every file twice -- once as `google/api/http.proto` and once as
            // `api_upstream/google/api/http.proto` -- and fail with "already defined".
            setSrcDirs(
                listOf(
                    vendoredProtos.dir("local"),
                    vendoredProtos.dir("api_upstream"),
                    vendoredProtos.dir("testsrv_upstream"),
                ),
            )
            // Exclude the google protobuf well-known types - use the runtime versions instead.
            // This prevents version conflicts between generated code and the protobuf-java runtime.
            exclude("**/google/protobuf/**")
        }
    }
}

// Directories vendored from the crate, and the only ones protoc reads.
val vendoredDirs = listOf("local", "api_upstream", "testsrv_upstream")

/**
 * Re-vendors the .proto files from a published `temporalio-protos` crate.
 *
 * Pass -PprotosCrateVersion=<x.y.z> to move to a different release; it defaults to the version
 * recorded in PINNED.toml. Crate tarballs on crates.io are immutable, so the version fully
 * determines the content. Maintainer-run: the resulting diff is meant to be reviewed, because a
 * proto rename would change JVM class names for every consumer.
 */
val syncProtosFromCrate by tasks.registering {
    description = "Re-vendor .proto files from a published temporalio-protos crate"
    group = "build setup"
    notCompatibleWithConfigurationCache("downloads and rewrites vendored sources")

    doLast {
        val pinned = file("PINNED.toml").readText()
        val current =
            Regex("""^version\s*=\s*"([^"]+)"""", RegexOption.MULTILINE)
                .find(pinned)
                ?.groupValues
                ?.get(1)
                ?: error("PINNED.toml has no version")
        val version = (project.findProperty("protosCrateVersion") as String?) ?: current

        val tarball =
            layout.buildDirectory
                .file("protos-crate/temporalio-protos-$version.crate")
                .get()
                .asFile
        tarball.parentFile.mkdirs()
        val url = "https://static.crates.io/crates/temporalio-protos/temporalio-protos-$version.crate"
        logger.lifecycle("Downloading $url")
        URI(url).toURL().openStream().use { input ->
            tarball.outputStream().use { input.copyTo(it) }
        }

        val extracted =
            layout.buildDirectory
                .dir("protos-crate/extracted")
                .get()
                .asFile
        extracted.deleteRecursively()
        copy {
            from(tarTree(resources.gzip(tarball)))
            into(extracted)
        }
        val crateRoot = File(extracted, "temporalio-protos-$version/protos")
        check(crateRoot.isDirectory) { "crate did not contain protos/ (looked in $crateRoot)" }

        val dest = vendoredProtos.asFile
        dest.deleteRecursively()
        vendoredDirs.forEach { dir ->
            copy {
                from(File(crateRoot, dir)) { include("**/*.proto") }
                into(File(dest, dir))
            }
        }

        file("protos.sha256").writeText(protoManifest(vendoredProtos.asFile))
        file(
            "PINNED.toml",
        ).writeText(pinned.replace(Regex("""^version\s*=.*$""", RegexOption.MULTILINE), """version = "$version""""))
        logger.lifecycle("Vendored temporalio-protos $version; review `git diff protos/` before committing.")
    }
}

/**
 * Fails if the vendored sources no longer match protos.sha256.
 *
 * Catches a hand-edit of generated-from-upstream content, which would silently diverge this
 * artifact's schema from the Core it claims to match.
 */
val verifyProtoPin by tasks.registering {
    description = "Verify vendored .proto files match protos.sha256"
    group = "verification"
    val manifest = file("protos.sha256")
    val protoRoot = vendoredProtos.asFile
    val protoFiles = fileTree(vendoredProtos) { include("**/*.proto") }
    inputs.file(manifest).withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.files(protoFiles).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.upToDateWhen { true }

    doLast {
        // Same normalisation as the hashes themselves: the manifest file may be checked out CRLF.
        val expected = manifest.readText().replace("\r\n", "\n")
        val actual = protoManifest(protoRoot)
        if (expected != actual) {
            val exp = expected.lines().filter { it.isNotBlank() }.toSet()
            val act = actual.lines().filter { it.isNotBlank() }.toSet()
            val changed = (exp - act).map { it.substringAfter("  ") }.sorted()
            val added = (act - exp).map { it.substringAfter("  ") }.sorted()
            error(
                "Vendored .proto files do not match protos.sha256.\n" +
                    "  changed or removed: ${changed.take(10)}\n" +
                    "  added or differing: ${added.take(10)}\n" +
                    "Run :protos:syncProtosFromCrate to re-vendor, or update protos.sha256 " +
                    "deliberately if you meant to edit them.",
            )
        }
    }
}

tasks.named("check") { dependsOn(verifyProtoPin) }

// Generated code is not documented. Suppressing the source sets keeps Dokka from walking ~2,100
// generated files, which is what forced `org.gradle.jvmargs=-Xmx8g` and the suppressedFiles
// workaround that used to live in core-bridge.
dokka {
    dokkaSourceSets.configureEach {
        suppress.set(true)
    }
}

mavenPublishing {
    // No Dokka javadoc jar: there is nothing here worth rendering, and building one over the
    // generated sources is slow and memory-hungry. The sources jar is kept so IDEs can navigate
    // into the generated code.
    configure(KotlinJvm(javadocJar = JavadocJar.Empty()))

    coordinates(artifactId = "protos")

    pom {
        name.set("Temporal KT Protos")
        description.set("Generated protobuf classes for the Temporal API and SDK-Core")
    }
}
