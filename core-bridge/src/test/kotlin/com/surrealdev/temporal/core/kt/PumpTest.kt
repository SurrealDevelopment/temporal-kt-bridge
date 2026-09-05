package com.surrealdev.temporal.core.kt

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Drives the whole stack from Kotlin: bindings, pump thread, and suspending requests.
 *
 * Loading [KtBridge] at all runs the ABI self-check. Nothing here registers an upcall, which is
 * the point: results arrive because this JVM thread asked for them.
 */
class PumpTest {
    private var runtime: Long = 0
    private lateinit var pump: Pump
    private val pushed = mutableListOf<Completion>()

    @Test
    fun `stopping the pump exposes public exceptions to pending and new callers`(): Unit =
        runBlocking {
            val waiting =
                async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                    assertFailsWith<com.surrealdev.temporal.core.TemporalCoreException> {
                        pump.request { /* Simulate an operation whose native completion never arrives. */ }
                    }
                }
            pump.close()
            assertEquals(KtBridge.KT_ERR_SHUTDOWN, waiting.await().statusCode)
            assertFailsWith<com.surrealdev.temporal.core.TemporalCoreException> {
                pump.request { error("a stopped pump must not issue requests") }
            }
        }

    @BeforeTest
    fun setUp() {
        runtime = KtBridge.runtimeNew(ByteArray(0))
        pump = Pump(runtime) { synchronized(pushed) { pushed += it } }
    }

    @AfterTest
    fun tearDown() {
        pump.close()
        KtBridge.runtimeFree(runtime)
    }

    @Test
    fun `a request suspends until its completion arrives`() =
        runBlocking {
            // Connecting to nothing still produces exactly one terminal completion, which is the
            // property that matters here -- not whether the connection succeeds.
            val completion =
                withTimeout(20_000) {
                    pump.request { reqId ->
                        KtBridge.clientConnect(runtime, clientConfig("http://127.0.0.1:1"), reqId)
                    }
                }
            assertTrue(completion.isFailure, "connecting to a closed port should fail")
            assertTrue(completion.errorMessage().isNotEmpty(), "a failure must carry a message")
        }

    // Note the explicit `Unit` return: an expression-bodied test whose last expression is
    // `assertFailsWith` returns the exception, and JUnit 5 silently ignores a test method that
    // does not return void -- it simply never runs, with no warning.
    @Test
    fun `cancelling a request does not leave the coroutine suspended`(): Unit =
        runBlocking {
            // Under the C bridge no native call could be cancelled at all: Rust always fired its
            // callback and held Arcs until it did. Here the request is cancellable because Rust
            // still answers exactly once and the answer is simply discarded.
            val job =
                async {
                    pump.request { reqId ->
                        KtBridge.clientConnect(runtime, clientConfig("http://10.255.255.1:7233"), reqId)
                    }
                }
            delay(200)
            job.cancel()
            assertFailsWith<CancellationException> { job.await() }
        }

    @Test
    fun `a failing start retires the request instead of hanging`(): Unit =
        runBlocking {
            // req_id 0 is reserved, so the native call rejects it synchronously. The suspending
            // request must surface that rather than wait for a completion that never comes.
            val error =
                withTimeout(5_000) {
                    assertFailsWith<com.surrealdev.temporal.core.TemporalCoreException> {
                        pump.request { KtBridge.clientConnect(runtime, ByteArray(0), 0) }
                    }
                }
            assertEquals(KtBridge.KT_ERR_INVALID_ARGUMENT, error.statusCode)
        }

    @Test
    fun `many concurrent requests each get exactly one answer`() =
        runBlocking {
            val jobs =
                (1..24).map {
                    async {
                        pump.request { reqId ->
                            KtBridge.clientConnect(runtime, clientConfig("http://127.0.0.1:1"), reqId)
                        }
                    }
                }
            val answers = withTimeout(30_000) { jobs.map { it.await() } }
            assertEquals(24, answers.size)
            assertEquals(24, answers.map { it.reqId }.toSet().size, "each request needs its own id")
        }

    @Test
    fun `a resumed coroutine can close its own pump`() =
        runBlocking {
            java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress()).use { socket ->
                val peer =
                    Thread.ofPlatform().start {
                        socket.accept().use {
                            // Keep the handshake pending until the requesting coroutine has suspended.
                            Thread.sleep(200)
                        }
                    }
                try {
                    val elapsed =
                        withTimeout(5_000) {
                            async(Dispatchers.Unconfined) {
                                pump.request { reqId ->
                                    KtBridge.clientConnect(
                                        runtime,
                                        clientConfig("http://127.0.0.1:${socket.localPort}"),
                                        reqId,
                                    )
                                }
                                assertTrue(Thread.currentThread().name.startsWith("temporal-pump-"))
                                val started = System.nanoTime()
                                pump.close()
                                (System.nanoTime() - started) / 1_000_000
                            }.await()
                        }
                    assertTrue(elapsed < 1_000, "closing on the pump thread took ${elapsed}ms")
                } finally {
                    peer.join(1_000)
                }
            }
        }

    /** Minimal `kt_bridge.ClientOptions`: field 1 is target_url. */
    private fun clientConfig(target: String): ByteArray {
        val value = target.toByteArray(Charsets.UTF_8)
        return byteArrayOf(0x0A, value.size.toByte()) + value
    }
}
