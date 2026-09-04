package com.surrealdev.temporal.core

import java.io.ByteArrayOutputStream

/**
 * Encodes `kt_bridge.EphemeralServerOptions` by hand.
 *
 * The bridge's own config protos are not published: `:protos` carries Temporal's schema, not this
 * crate's private messages, and a handful of fields does not justify a second generated artifact.
 */
internal object EphemeralServerOptionsProto {
    @Suppress("LongParameterList")
    fun encode(
        existingPath: String?,
        downloadVersion: String?,
        namespace: String,
        ip: String,
        extraArgs: List<String>,
        testServer: Boolean,
        logFile: String?,
    ): ByteArray {
        val out = ByteArrayOutputStream()

        fun varint(value: Int) {
            var v = value
            while (v >= 0x80) {
                out.write((v and 0x7F) or 0x80)
                v = v ushr 7
            }
            out.write(v)
        }

        fun string(
            number: Int,
            value: String?,
        ) {
            if (value.isNullOrEmpty()) return
            val bytes = value.toByteArray(Charsets.UTF_8)
            out.write((number shl 3) or 2)
            varint(bytes.size)
            out.write(bytes)
        }

        string(1, existingPath)
        string(2, downloadVersion)
        string(6, namespace)
        string(7, ip)
        extraArgs.forEach { string(10, it) }
        if (testServer) {
            out.write((11 shl 3) or 0)
            varint(1)
        }
        string(12, logFile)
        return out.toByteArray()
    }
}
