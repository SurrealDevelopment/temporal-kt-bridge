package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.kt.KtRuntime

/**
 * A Temporal Core runtime.
 *
 * Owns the native runtime and the single pump thread that drains its completion queue. Everything
 * else -- clients, workers, dev servers -- is created from one of these and shares its queue.
 *
 * ```kotlin
 * TemporalRuntime.create().use { runtime ->
 *     val client = TemporalCoreClient.connect(runtime, ClientOptions(targetUrl = "http://localhost:7233"))
 * }
 * ```
 *
 * @throws TemporalCoreException if the runtime cannot be created
 */
class TemporalRuntime private constructor(
    internal val kt: KtRuntime,
) : AutoCloseable {
    @Volatile
    private var closed = false

    companion object {
        /** Creates a runtime with default options. */
        fun create(): TemporalRuntime = create(coreMetricsMeter = null)

        /**
         * Creates a runtime.
         *
         * [coreMetricsMeter] is accepted for source compatibility but no longer used: Core's own
         * Prometheus and OTLP exporters replace the eight custom-meter callbacks the previous
         * bridge ran on Tokio threads, each of which needed a blanket `catch (Throwable)` to stop
         * an exception crossing the FFI boundary. Passing a meter has no effect.
         */
        @Suppress("UNUSED_PARAMETER")
        fun create(
            coreMetricsMeter: Any?,
            workerHeartbeatIntervalMs: Long = 60_000L,
        ): TemporalRuntime = TemporalRuntime(KtRuntime.create())
    }

    fun isClosed(): Boolean = closed

    /**
     * Closes the runtime.
     *
     * Freeing the native runtime answers every outstanding request and closes the completion
     * queue, which releases the pump. Nothing is left waiting, so unlike the previous bridge
     * there is no 60-second latch here.
     */
    override fun close() {
        if (closed) return
        synchronized(this) {
            if (closed) return
            closed = true
            kt.close()
        }
    }

    internal fun ensureOpen() {
        check(!closed) { "Runtime has been closed" }
    }
}
