package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.proto.EphemeralServerOptions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class EphemeralServerOptionsProtoTest {
    private fun options(ttl: Long): ByteArray =
        EphemeralServerOptionsProto.encode(null, "default", ttl, "default", "127.0.0.1", emptyList(), false, null)

    @Test
    fun `download lifetime preserves zero and explicit values and rejects negatives`() {
        for (ttl in listOf(0L, 123L, Long.MAX_VALUE)) {
            val decoded = EphemeralServerOptions.parseFrom(options(ttl))
            assertTrue(decoded.hasDownloadTtlSeconds())
            assertEquals(ttl, decoded.downloadTtlSeconds)
        }
        assertFailsWith<IllegalArgumentException> { options(-1) }
    }
}
