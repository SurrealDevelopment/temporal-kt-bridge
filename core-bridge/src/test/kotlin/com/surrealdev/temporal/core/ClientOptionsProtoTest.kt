package com.surrealdev.temporal.core

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ClientOptionsProtoTest {
    @Test
    fun `TLS options determine both scheme and TLS config`() {
        for (target in listOf(
            "localhost:7233",
            "http://localhost:7233",
            "https://localhost:7233",
            "HTTPS://localhost:7233",
        )) {
            val disabled = clientConfig(target, "ns", tls = TlsConfig(), apiKey = "secret", tlsDisabled = true)
            assertEquals("http://localhost:7233", disabled.targetUrl)
            assertFalse(disabled.tls)
            for (config in listOf(
                clientConfig(target, "ns", tls = TlsConfig()),
                clientConfig(target, "ns", apiKey = "secret"),
            )) {
                assertEquals("https://localhost:7233", config.targetUrl)
                assertTrue(config.tls)
            }
        }
        assertFalse(clientConfig("http://localhost:7233", "ns").tls)
        assertTrue(clientConfig("https://localhost:7233", "ns").tls)
    }

    @Test
    fun `client settings and TLS material reach the generated config`() {
        val tls = TlsConfig(byteArrayOf(1), "cloud.example", byteArrayOf(2), byteArrayOf(3))
        val config =
            clientConfig(
                "cloud.example:7233",
                "ns",
                ClientOptions("test-client", "1.2.3", "me@host", GrpcCompression.NONE, 500),
                tls,
                "secret",
            )
        assertEquals("test-client", config.clientName)
        assertEquals("1.2.3", config.clientVersion)
        assertEquals("me@host", config.identity)
        assertEquals("ns", config.namespace)
        assertEquals("secret", config.apiKey)
        assertEquals(500L, config.connectTimeoutMillis)
        assertTrue(config.noCompression)
        assertContentEquals(tls.serverRootCaCert, config.serverRootCaCert.toByteArray())
        assertEquals(tls.domain, config.tlsDomain)
        assertContentEquals(tls.clientCert, config.clientCert.toByteArray())
        assertContentEquals(tls.clientPrivateKey, config.clientPrivateKey.toByteArray())
        val disabled = clientConfig("https://cloud.example:7233", "ns", tls = tls, tlsDisabled = true)
        assertTrue(
            disabled.serverRootCaCert.isEmpty && disabled.clientCert.isEmpty && disabled.clientPrivateKey.isEmpty,
        )
        assertEquals("", disabled.tlsDomain)
        assertFailsWith<IllegalArgumentException> { ClientOptions(connectTimeoutMillis = -1) }
    }
}
