package com.surrealdev.temporal.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Wire-level check of the hand-rolled `kt_bridge.ClientOptions` encoder.
 *
 * Every field here was at some point accepted by the Kotlin API and silently not sent -- the
 * worker connected fine and Core substituted its own default (`client-name: temporal-rust`, gzip
 * on). This pins what actually leaves the JVM.
 */
class ClientOptionsProtoTest {
    private fun fields(bytes: ByteArray): Map<Int, ByteArray> {
        val out = mutableMapOf<Int, ByteArray>()
        var i = 0

        fun varint(): Int {
            var r = 0
            var shift = 0
            while (true) {
                val b = bytes[i++].toInt() and 0xFF
                r = r or ((b and 0x7F) shl shift)
                if (b < 0x80) return r
                shift += 7
            }
        }
        while (i < bytes.size) {
            val tag = varint()
            val number = tag ushr 3
            out[number] =
                when (tag and 7) {
                    0 -> {
                        val s = i
                        varint()
                        bytes.copyOfRange(s, i)
                    }
                    2 -> {
                        val len = varint()
                        bytes.copyOfRange(i, i + len).also { i += len }
                    }
                    else -> error("unexpected wire type for field $number")
                }
        }
        return out
    }

    @Test
    fun `identity, client name and version, api key and compression reach the wire`() {
        val f =
            fields(
                ClientOptionsProto.encode(
                    targetUrl = "http://localhost:7233",
                    namespace = "ns",
                    identity = "me@host",
                    apiKey = "secret",
                    noCompression = true,
                    clientName = "temporal-kotlin",
                    clientVersion = "1.2.3",
                ),
            )
        assertEquals("http://localhost:7233", String(f.getValue(1)))
        assertEquals("ns", String(f.getValue(2)))
        assertEquals("me@host", String(f.getValue(3)))
        assertEquals("temporal-kotlin", String(f.getValue(4)))
        assertEquals("1.2.3", String(f.getValue(5)))
        assertEquals("secret", String(f.getValue(6)))
        assertEquals(1, f.getValue(8)[0].toInt(), "no_compression")
    }

    @Test
    fun `defaults send nothing, so Core's own defaults apply`() {
        val f = fields(ClientOptionsProto.encode("http://x", "ns", identity = "", apiKey = ""))
        assertTrue(3 !in f && 4 !in f && 5 !in f && 6 !in f && 8 !in f, "unset options must not be sent")
    }

    @Test
    fun `tls flag and material reach the wire`() {
        val f =
            fields(
                ClientOptionsProto.encode(
                    targetUrl = "https://cloud.example:7233",
                    namespace = "ns",
                    identity = "",
                    apiKey = "",
                    tls = true,
                    tlsConfig =
                        TlsConfig(
                            serverRootCaCert = byteArrayOf(1, 2, 3),
                            domain = "cloud.example",
                            clientCert = byteArrayOf(4),
                            clientPrivateKey = byteArrayOf(5, 6),
                        ),
                ),
            )
        assertEquals(1, f.getValue(9)[0].toInt(), "tls")
        assertEquals(listOf<Byte>(1, 2, 3), f.getValue(10).toList())
        assertEquals("cloud.example", String(f.getValue(11)))
        assertEquals(listOf<Byte>(4), f.getValue(12).toList())
        assertEquals(listOf<Byte>(5, 6), f.getValue(13).toList())
    }

    @Test
    fun `plain http sends no tls at all`() {
        val f = fields(ClientOptionsProto.encode("http://localhost:7233", "ns", identity = "", apiKey = ""))
        assertTrue((9..13).none { it in f }, "an http:// target must not turn TLS on")
    }
}
