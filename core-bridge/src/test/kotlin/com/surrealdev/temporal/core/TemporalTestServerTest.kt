package com.surrealdev.temporal.core

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds

/**
 * The time-skipping test server, which had no coverage here at all.
 *
 * That gap let a wrong download version ship: the test server is a separate artifact from the
 * Temporal CLI and does not share its version line, so passing a CLI tag 404s on
 * temporal.download. Nothing in this module noticed, and every downstream integration test failed
 * at once.
 */
class TemporalTestServerTest {
    @Test
    fun `sleep durations preserve large exact values and reject invalid values`() {
        val longDuration = 146_000.days
        val encoded = sleepRequest(longDuration).duration
        assertEquals(longDuration.inWholeSeconds, encoded.seconds)
        assertEquals(0, encoded.nanos)
        assertEquals(250_000_000, sleepRequest(1.25.seconds).duration.nanos)
        assertEquals(0, sleepRequest(Duration.ZERO).duration.nanos)
        for (invalid in listOf((-1).seconds, Duration.INFINITE, 315_576_000_001L.seconds)) {
            assertFailsWith<IllegalArgumentException> { sleepRequest(invalid) }
        }
    }

    @Test
    fun `custom search attributes are registered on the time skipping server`() =
        runBlocking {
            TemporalRuntime.create().use { runtime ->
                TemporalTestServer
                    .start(
                        runtime,
                        searchAttributes = listOf("ReviewKeyword" to "Keyword"),
                    ).use { server ->
                        TemporalCoreClient.connect(runtime, server.targetUrl).use { client ->
                            val value =
                                io.temporal.api.common.v1.Payload
                                    .newBuilder()
                                    .putMetadata(
                                        "encoding",
                                        com.google.protobuf.ByteString
                                            .copyFromUtf8("json/plain"),
                                    ).putMetadata(
                                        "type",
                                        com.google.protobuf.ByteString
                                            .copyFromUtf8("Keyword"),
                                    ).setData(
                                        com.google.protobuf.ByteString
                                            .copyFromUtf8("\"review\""),
                                    )
                            val request =
                                io.temporal.api.workflowservice.v1.StartWorkflowExecutionRequest
                                    .newBuilder()
                                    .setNamespace("default")
                                    .setWorkflowId("review-search-attributes")
                                    .setRequestId("review-request")
                                    .setWorkflowType(
                                        io.temporal.api.common.v1.WorkflowType
                                            .newBuilder()
                                            .setName("Review"),
                                    ).setTaskQueue(
                                        io.temporal.api.taskqueue.v1.TaskQueue
                                            .newBuilder()
                                            .setName("review"),
                                    ).setSearchAttributes(
                                        io.temporal.api.common.v1.SearchAttributes
                                            .newBuilder()
                                            .putIndexedFields("ReviewKeyword", value.build()),
                                    ).build()
                            val started =
                                client.workflowServiceCall("StartWorkflowExecution", request) {
                                    io.temporal.api.workflowservice.v1.StartWorkflowExecutionResponse
                                        .parseFrom(it)
                                }
                            assertTrue(
                                started.runId.isNotEmpty(),
                                "workflow must accept the registered custom attribute",
                            )
                        }
                    }
            }
        }

    @Test
    fun `starts, skips time and shuts down`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                TemporalTestServer.start(runtime).use { server ->
                    assertTrue(server.targetUrl.isNotEmpty(), "the server must report a target")
                    assertNotNull(server.pid, "a running server has a pid")

                    // The point of this server: time is under the caller's control. The server
                    // starts with skipping locked -- an SDK unlocks it once it is waiting on
                    // something -- and Sleep with the lock held waits in real time, so the RPC
                    // would simply time out.
                    server.unlockTimeSkipping()
                    val before = server.getCurrentTime()
                    server.sleep(kotlin.time.Duration.parse("1h"))
                    val after = server.getCurrentTime()
                    assertTrue(
                        after.isAfter(before.plusSeconds(3000)),
                        "sleep(1h) should move the clock roughly an hour, was $before -> $after",
                    )
                    server.lockTimeSkipping()
                }
            }
        }

    @Test
    fun `pid is null once closed`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                val server = TemporalTestServer.start(runtime)
                assertNotNull(server.pid)
                server.close()
                assertNull(server.pid, "a stopped server must not report a pid the OS can reuse")
            }
        }
}
