package com.surrealdev.temporal.core

import com.google.protobuf.Empty
import io.temporal.api.testservice.v1.GetCurrentTimeResponse
import io.temporal.api.testservice.v1.LockTimeSkippingRequest
import io.temporal.api.testservice.v1.LockTimeSkippingResponse
import io.temporal.api.testservice.v1.SleepRequest
import io.temporal.api.testservice.v1.SleepResponse
import io.temporal.api.testservice.v1.SleepUntilRequest
import io.temporal.api.testservice.v1.UnlockTimeSkippingRequest
import io.temporal.api.testservice.v1.UnlockTimeSkippingResponse
import java.time.Instant
import kotlin.time.Duration

/**
 * An ephemeral Temporal test server with time-skipping support.
 *
 * This server is useful for testing workflows with long timers. It runs an
 * in-memory Temporal test server that can automatically skip time when all
 * workflows are waiting on timers, allowing tests with timers of hours or
 * days to complete in milliseconds.
 *
 * The test server starts with time-skipping locked (disabled). Call
 * [unlockTimeSkipping] to enable automatic time advancement, or use
 * [sleepUntil] and [sleep] for precise time control.
 *
 * For local development without time-skipping, use [TemporalDevServer] instead.
 *
 * Example usage:
 * ```kotlin
 * TemporalRuntime.create().use { runtime ->
 *     TemporalTestServer.start(runtime).use { server ->
 *         println("Test server running at: ${server.targetUrl}")
 *
 *         // Enable automatic time skipping
 *         server.unlockTimeSkipping()
 *
 *         // Or manually advance time
 *         server.lockTimeSkipping()
 *         server.sleepUntil(Instant.now().plusSeconds(3600)) // Skip 1 hour
 *     }
 * }
 * ```
 *
 * The time-skipping test server.
 *
 * Its own embedded client issues the TestService RPCs, so callers do not have to hold one.
 */
class TemporalTestServer private constructor(
    private val kt: com.surrealdev.temporal.core.kt.KtEphemeralServer,
    private val client: TemporalCoreClient,
) : EphemeralServer {
    @Volatile
    private var closed = false

    @Volatile
    private var shutDown = false

    override val targetUrl: String get() = kt.target

    /**
     * The OS pid of the server process, or null once it has been shut down.
     *
     * Null after [close] on purpose: the process is gone, and the OS is free to hand that number
     * to something else.
     */
    override val pid: Long? get() =
        if (closed ||
            shutDown ||
            kt.runtimeClosed
        ) {
            null
        } else {
            kt.pid.takeIf { it > 0 }?.toLong()
        }

    override fun isClosed(): Boolean = closed || kt.runtimeClosed

    companion object {
        @Suppress("LongParameterList")
        suspend fun start(
            runtime: TemporalRuntime,
            existingPath: String? = null,
            downloadVersion: String = "default",
            // Zero keeps cached downloads indefinitely.
            downloadTtlSeconds: Long = 0,
            searchAttributes: List<Pair<String, String>> = emptyList(),
            extraArgs: List<String> = emptyList(),
            logFile: String? = null,
        ): TemporalTestServer {
            runtime.ensureOpen()
            val allArgs =
                buildList {
                    searchAttributes.forEach { (name, type) ->
                        add("--search-attribute")
                        add("$name=$type")
                    }
                    addAll(extraArgs)
                }
            val server =
                com.surrealdev.temporal.core.kt.KtEphemeralServer.start(
                    runtime.kt,
                    EphemeralServerOptionsProto.encode(
                        existingPath = existingPath,
                        // NOT the Temporal CLI version: the time-skipping test server is a
                        // separate artifact on its own version line, so a CLI tag like v1.6.1
                        // 404s on temporal.download. "default" lets Core pick the version it was
                        // built against.
                        downloadVersion = downloadVersion,
                        downloadTtlSeconds = downloadTtlSeconds,
                        namespace = "default",
                        ip = "127.0.0.1",
                        extraArgs = allArgs,
                        testServer = true,
                        logFile = logFile,
                    ),
                )
            val client =
                try {
                    TemporalCoreClient.connect(runtime, server.target, "default")
                } catch (e: Throwable) {
                    // Do not leak the server process if its client cannot connect.
                    server.shutdown()
                    server.close()
                    throw e
                }
            return TemporalTestServer(server, client).also { EphemeralServers.register(it) }
        }
    }

    suspend fun lockTimeSkipping() {
        client.testServiceCall("LockTimeSkipping", LockTimeSkippingRequest.getDefaultInstance()) {
            LockTimeSkippingResponse.parseFrom(it)
        }
    }

    suspend fun unlockTimeSkipping() {
        client.testServiceCall("UnlockTimeSkipping", UnlockTimeSkippingRequest.getDefaultInstance()) {
            UnlockTimeSkippingResponse.parseFrom(it)
        }
    }

    suspend fun unlockTimeSkippingWithSleep(duration: Duration) {
        val request =
            SleepRequest
                .newBuilder()
                .setDuration(
                    com.google.protobuf.Duration
                        .newBuilder()
                        .setSeconds(duration.inWholeSeconds)
                        .setNanos((duration.inWholeNanoseconds % 1_000_000_000L).toInt()),
                ).build()
        client.testServiceCall("UnlockTimeSkippingWithSleep", request) { SleepResponse.parseFrom(it) }
    }

    suspend fun getCurrentTime(): Instant {
        // GetCurrentTime takes an empty request; the bridge dispatches unit-request RPCs
        // separately because there is nothing to decode.
        val response =
            client.testServiceCall("GetCurrentTime", Empty.getDefaultInstance()) {
                GetCurrentTimeResponse.parseFrom(it)
            }
        return Instant.ofEpochSecond(response.time.seconds, response.time.nanos.toLong())
    }

    suspend fun sleep(duration: Duration) {
        val request =
            SleepRequest
                .newBuilder()
                .setDuration(
                    com.google.protobuf.Duration
                        .newBuilder()
                        .setSeconds(duration.inWholeSeconds)
                        .setNanos((duration.inWholeNanoseconds % 1_000_000_000L).toInt()),
                ).build()
        client.testServiceCall("Sleep", request) { SleepResponse.parseFrom(it) }
    }

    suspend fun sleepUntil(time: Instant) {
        val request =
            SleepUntilRequest
                .newBuilder()
                .setTimestamp(
                    com.google.protobuf.Timestamp
                        .newBuilder()
                        .setSeconds(time.epochSecond)
                        .setNanos(time.nano),
                ).build()
        client.testServiceCall("SleepUntil", request) { SleepResponse.parseFrom(it) }
    }

    suspend fun shutdown() {
        if (closed) return
        shutDown = true
        kt.shutdown()
    }

    override fun close() {
        if (closed) return
        synchronized(this) {
            if (closed) return
            closed = true
            client.close()
            kt.close()
            EphemeralServers.unregister(this)
        }
    }
}
