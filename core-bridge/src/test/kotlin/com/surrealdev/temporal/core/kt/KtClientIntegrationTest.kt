package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException
import io.temporal.api.workflowservice.v1.DescribeNamespaceRequest
import io.temporal.api.workflowservice.v1.DescribeNamespaceResponse
import io.temporal.api.workflowservice.v1.StartWorkflowExecutionRequest
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A real Temporal RPC driven entirely through the Kotlin stack on the new bridge.
 *
 * This is the first point where actual SDK-shaped work runs end to end with no FFM upcall
 * anywhere: Kotlin issues a request, Rust answers it on the completion queue, and the pump
 * resumes the coroutine.
 *
 * Set TEMPORAL_TEST_ADDRESS to run; skipped loudly otherwise.
 */
class KtClientIntegrationTest {
    private fun address(): String? = System.getenv("TEMPORAL_TEST_ADDRESS")?.takeIf { it.isNotEmpty() }

    @Test
    fun `describe namespace round trips through the pump`() {
        val address =
            address() ?: run {
                println("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server")
                return
            }
        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(30_000) {
                    KtClient.connect(runtime, clientOptions(address)).use { client ->
                        val request =
                            DescribeNamespaceRequest
                                .newBuilder()
                                .setNamespace("default")
                                .build()
                        val bytes = client.call(KtService.WORKFLOW, "DescribeNamespace", request.toByteArray())

                        val response = DescribeNamespaceResponse.parseFrom(bytes)
                        assertEquals(
                            "default",
                            response.namespaceInfo.name,
                            "the response must decode as the type the RPC name implies",
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `a server rejection surfaces the gRPC status, not a generic failure`() {
        val address =
            address() ?: run {
                println("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server")
                return
            }
        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(30_000) {
                    KtClient.connect(runtime, clientOptions(address)).use { client ->
                        val error =
                            assertFailsWith<TemporalCoreException> {
                                // An empty request the server must reject.
                                client.call(
                                    KtService.WORKFLOW,
                                    "StartWorkflowExecution",
                                    StartWorkflowExecutionRequest.newBuilder().build().toByteArray(),
                                )
                            }
                        // 3 == INVALID_ARGUMENT. A positive code means it came from the server.
                        assertTrue(
                            error.statusCode != null && error.statusCode!! > 0,
                            "expected a gRPC status code, got ${error.statusCode}",
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `connecting to a closed port fails rather than hanging`() {
        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(30_000) {
                    assertFailsWith<TemporalCoreException> {
                        KtClient.connect(runtime, clientOptions("127.0.0.1:1"))
                    }
                }
            }
        }
    }

    @Test
    fun `cancellation after a success is dispatched releases the client handle`() {
        val address =
            address() ?: run {
                println("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server")
                return
            }
        KtRuntime.create().use { runtime ->
            runBlocking {
                KtClient.connect(runtime, clientOptions(address)).use { client ->
                    val ready = java.util.ArrayDeque<Runnable>()
                    val dispatcher =
                        java.util.concurrent
                            .Executor { ready.add(it) }
                            .asCoroutineDispatcher()
                    var reqId = 0L
                    val waiter = async(dispatcher) { runtime.pump.request { reqId = it } }
                    ready.removeFirst().run()
                    // Transfer a real native resource, then cancel before the dispatcher delivers it.
                    runtime.pump.dispatch(
                        Completion(reqId, Kind.CLIENT_CONNECTED, KtBridge.KT_OK, ByteArray(0), client.handle, 0),
                    )
                    waiter.cancel()
                    ready.removeFirst().run()
                    assertFailsWith<CancellationException> { waiter.await() }
                    assertEquals(KtBridge.KT_ERR_STALE_HANDLE, KtBridge.clientFree(client.handle))
                }
            }
        }
    }

    /** Minimal `kt_bridge.ClientOptions`: field 1 target_url, field 2 namespace. */
    private fun clientOptions(address: String): ByteArray {
        val url = "http://$address".toByteArray(Charsets.UTF_8)
        val ns = "default".toByteArray(Charsets.UTF_8)
        return byteArrayOf(0x0A, url.size.toByte()) + url + byteArrayOf(0x12, ns.size.toByte()) + ns
    }
}
