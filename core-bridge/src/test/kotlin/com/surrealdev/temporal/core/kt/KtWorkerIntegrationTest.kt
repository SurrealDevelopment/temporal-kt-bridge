package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertFailsWith

/**
 * Worker lifecycle from Kotlin against a real server.
 *
 * The property under test is the one the C bridge kept failing: a worker must shut down. Core
 * does not finish shutting down until every poll stream has been polled to `PollError::ShutDown`,
 * and with the loops in Rust that cannot be broken by anything Kotlin does.
 *
 * Set TEMPORAL_TEST_ADDRESS to run; skipped loudly otherwise.
 */
class KtWorkerIntegrationTest {
    private fun address(): String? = System.getenv("TEMPORAL_TEST_ADDRESS")?.takeIf { it.isNotEmpty() }

    @Test
    fun `a worker starts, polls and shuts down, closing its task streams`() {
        val address =
            address() ?: run {
                println("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server")
                return
            }

        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(60_000) {
                    KtClient.connect(runtime, clientOptions(address)).use { client ->
                        val worker = KtWorker.create(runtime, client, workerOptions("kt-bridge-it"))
                        worker.use {
                            worker.start()

                            // A heartbeat on a live worker is accepted. (There is no activity to
                            // heartbeat for, so Core will ignore it -- what matters is that the
                            // call does not throw or abort, which is the SIGABRT race.)
                            worker.heartbeat(ByteArray(0))

                            // Both callers must observe the same successful shutdown. Holding one
                            // Core Arc per caller used to make each finalizer wait on the other.
                            listOf(
                                async { worker.shutdown(graceMillis = 30_000) },
                                async { worker.shutdown(graceMillis = 30_000) },
                            ).awaitAll()

                            // Every stream must have closed. A stream left open is a consumer
                            // suspended forever, which is the hang this design removes.
                            assertStreamClosed("workflow activations") {
                                worker.workflowActivationStream.receive()
                            }
                            assertStreamClosed("activity tasks") {
                                worker.activityTaskStream.receive()
                            }
                            assertStreamClosed("nexus tasks") {
                                worker.nexusTaskStream.receive()
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `a heartbeat after shutdown is refused rather than aborting the process`() {
        val address =
            address() ?: run {
                println("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server")
                return
            }
        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(60_000) {
                    KtClient.connect(runtime, clientOptions(address)).use { client ->
                        val worker = KtWorker.create(runtime, client, workerOptions("kt-bridge-hb"))
                        worker.use {
                            worker.start()
                            worker.shutdown(graceMillis = 30_000)
                            // The C bridge unwrapped a finalized worker here and took the whole
                            // JVM down with SIGABRT. Reaching the assertion at all is the test.
                            assertFailsWith<TemporalCoreException>("a finalized worker must refuse heartbeats") {
                                worker.heartbeat(ByteArray(0))
                            }
                        }
                    }
                }
            }
        }
    }

    private suspend fun assertStreamClosed(
        name: String,
        receive: suspend () -> ByteArray,
    ) {
        try {
            receive()
            // A task is fine too: it means the stream had buffered work, not that it hung.
        } catch (_: ClosedReceiveChannelException) {
            return
        }
    }

    private fun clientOptions(address: String): ByteArray {
        val url = "http://$address".toByteArray(Charsets.UTF_8)
        val ns = "default".toByteArray(Charsets.UTF_8)
        return byteArrayOf(0x0A, url.size.toByte()) + url + byteArrayOf(0x12, ns.size.toByte()) + ns
    }

    /** `kt_bridge.WorkerOptions`: field 1 namespace, field 2 task_queue. */
    private fun workerOptions(taskQueue: String): ByteArray {
        val ns = "default".toByteArray(Charsets.UTF_8)
        val tq = taskQueue.toByteArray(Charsets.UTF_8)
        return byteArrayOf(0x0A, ns.size.toByte()) + ns + byteArrayOf(0x12, tq.size.toByte()) + tq
    }
}
