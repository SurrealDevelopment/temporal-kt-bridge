package com.surrealdev.temporal.core

import io.temporal.api.testservice.v1.SleepRequest
import io.temporal.api.testservice.v1.SleepResponse
import io.temporal.api.workflowservice.v1.GetSystemInfoRequest
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The per-call timeout is what makes a bounded long poll possible.
 *
 * It was accepted and silently dropped, so every `waitNewEvent` poll blocked until something
 * unrelated gave up, and the caller -- which branches on DEADLINE_EXCEEDED to mean "window
 * elapsed, look again" -- saw an opaque UNKNOWN instead.
 */
class RpcTimeoutTest {
    @Test
    fun `a call that outlives its timeout is cut off with a deadline status`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                TemporalTestServer.start(runtime).use { server ->
                    TemporalCoreClient.connect(runtime, server.targetUrl, "default").use { client ->
                        // With time skipping locked (the server's initial state), Sleep waits in
                        // real time -- a server-side block whose only way out is our deadline.
                        val started = System.nanoTime()
                        val error =
                            assertFailsWith<TemporalCoreException> {
                                client.testServiceCall(
                                    "Sleep",
                                    SleepRequest
                                        .newBuilder()
                                        .setDuration(
                                            com.google.protobuf.Duration
                                                .newBuilder()
                                                .setSeconds(30),
                                        ).build(),
                                    timeoutMillis = 1_000,
                                ) { SleepResponse.parseFrom(it) }
                            }
                        val elapsedMs = (System.nanoTime() - started) / 1_000_000

                        // tonic reports its own client-side deadline as CANCELLED ("Timeout
                        // expired") rather than DEADLINE_EXCEEDED, which is what a server-enforced
                        // deadline would produce. Callers treat both as "the window elapsed"; what
                        // must never come back is UNKNOWN, which is what an unbounded call died with.
                        assertTrue(
                            error.statusCode == GRPC_CANCELLED || error.statusCode == GRPC_DEADLINE_EXCEEDED,
                            "expected CANCELLED or DEADLINE_EXCEEDED, got ${error.statusCode}: ${error.message}",
                        )
                        assertTrue(elapsedMs >= 900, "the call returned before its window, in ${elapsedMs}ms")
                        assertTrue(elapsedMs < 10_000, "the deadline must bound the call, took ${elapsedMs}ms")
                    }
                }
            }
        }

    @Test
    fun `a timeout of zero means no deadline`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                TemporalDevServer.start(runtime).use { server ->
                    TemporalCoreClient.connect(runtime, server.targetUrl, "default").use { client ->
                        // A prompt RPC on the default path: proves it is not deadlined into failure.
                        val response =
                            client.workflowServiceCall(
                                "GetSystemInfo",
                                GetSystemInfoRequest.getDefaultInstance(),
                            ) { GetSystemInfoResponse.parseFrom(it) }
                        assertTrue(response.serverVersion.isNotEmpty())
                    }
                }
            }
        }

    private companion object {
        const val GRPC_CANCELLED = 1
        const val GRPC_DEADLINE_EXCEEDED = 4
    }
}
