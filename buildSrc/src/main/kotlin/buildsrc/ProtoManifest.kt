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
            // Hashed with line endings normalised to LF: a CRLF checkout (Windows with
            // core.autocrlf) must produce the same manifest as the LF one it was pinned from.
            // .gitattributes now forces LF for .proto, but the check should not depend on it.
            val digest =
                MessageDigest
                    .getInstance("SHA-256")
                    .digest(file.readText(Charsets.UTF_8).replace("\r\n", "\n").toByteArray(Charsets.UTF_8))
                    .joinToString("") { byte -> "%02x".format(byte) }
            "$digest  ${file.relativeTo(root).invariantSeparatorsPath}"
        }.sortedBy { it.substringAfter("  ") }
        .joinToString("\n", postfix = "\n")
