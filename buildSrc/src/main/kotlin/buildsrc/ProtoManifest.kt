package buildsrc

import java.io.File
import java.security.MessageDigest

/**
 * SHA-256 manifest of every `.proto` under [root], sorted by path.
 *
 * Lives in buildSrc rather than in the build script because tasks capture it: a function defined
 * in a `.gradle.kts` script carries a reference to the script object, which the configuration
 * cache cannot serialize.
 */
fun protoManifest(root: File): String =
    root
        .walkTopDown()
        .filter { it.isFile && it.extension == "proto" }
        .map { file ->
            val digest =
                MessageDigest
                    .getInstance("SHA-256")
                    .digest(file.readBytes())
                    .joinToString("") { byte -> "%02x".format(byte) }
            "$digest  ${file.relativeTo(root).invariantSeparatorsPath}"
        }.sortedBy { it.substringAfter("  ") }
        .joinToString("\n", postfix = "\n")
