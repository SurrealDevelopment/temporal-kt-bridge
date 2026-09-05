package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

/** Which task stream a completion belongs to. Must match `TaskKind` in worker.rs. */
internal enum class KtTaskKind(
    val code: Int,
) {
    WORKFLOW_ACTIVATION(0),
    ACTIVITY(1),
    NEXUS(2),
    ;

    companion object {
        fun fromAux(aux1: Long): KtTaskKind? = entries.firstOrNull { it.code.toLong() == aux1 }
    }
}

/**
 * A worker on the kt-bridge ABI.
 *
 * Tasks arrive as pushed completions and land on channels. Kotlin no longer runs poll loops at
 * all, which is the point: Core requires the language side to keep polling until it reports
 * `ShutDown`, and under the C bridge that contract leaked into `ManagedWorker` as a
 * cancellation-safe re-poll loop. Any future path that cancelled the poll coroutine reintroduced
 * an unkillable hang. Cancelling a consumer of these channels cannot break Core, because the
 * loops live in Rust.
 *
 * Each channel closes when its stream reports `ShutDown`, so a consumer sees a normal end rather
 * than waiting forever.
 */
internal class KtWorker private constructor(
    private val runtime: KtRuntime,
    val handle: Long,
) : AutoCloseable {
    private val workflowActivations = Channel<ByteArray>(Channel.UNLIMITED)
    private val activityTasks = Channel<ByteArray>(Channel.UNLIMITED)
    private val nexusTasks = Channel<ByteArray>(Channel.UNLIMITED)

    @Volatile
    private var closed = false

    val workflowActivationStream: ReceiveChannel<ByteArray> get() = workflowActivations
    val activityTaskStream: ReceiveChannel<ByteArray> get() = activityTasks
    val nexusTaskStream: ReceiveChannel<ByteArray> get() = nexusTasks

    private fun onEvent(completion: Completion) {
        when (completion.kind) {
            Kind.TASK_WORKFLOW_ACTIVATION -> workflowActivations.trySend(completion.payload)
            Kind.TASK_ACTIVITY -> activityTasks.trySend(completion.payload)
            Kind.TASK_NEXUS -> nexusTasks.trySend(completion.payload)
            Kind.TASK_STREAM_END -> channelFor(completion.aux1)?.close()
            Kind.WORKER_FAILED -> {
                // aux1 names the stream, so the right consumer is unblocked rather than all of
                // them being left waiting on a worker that is no longer polling.
                val failure =
                    TemporalCoreException(
                        message = "worker poll stream failed: ${completion.errorMessage()}",
                        errorType = null,
                        statusCode = completion.status,
                        cause = null,
                        writableStackTrace = true,
                    )
                channelFor(completion.aux1)?.close(failure)
            }
            else -> Unit
        }
    }

    private fun channelFor(aux1: Long): Channel<ByteArray>? =
        when (KtTaskKind.fromAux(aux1)) {
            KtTaskKind.WORKFLOW_ACTIVATION -> workflowActivations
            KtTaskKind.ACTIVITY -> activityTasks
            KtTaskKind.NEXUS -> nexusTasks
            null -> null
        }

    /** Starts the Rust-side poll loops. Idempotent. */
    fun start() {
        runtime.ensureOpen()
        KtBridge.workerStart(runtime.handle, handle)
    }

    // complete() and shutdown() run under NonCancellable. Cancelling a request drops Core's
    // future mid-flight (the bridge races the cancellation token against the operation), and
    // Core's completion path is not cancel-safe: the activation stays outstanding until the
    // workflow task times out and its slot may never be released. A cancelled caller still gets
    // its CancellationException afterwards; the operation simply finishes first.
    suspend fun complete(
        kind: KtTaskKind,
        proto: ByteArray,
    ) {
        runtime.ensureOpen()
        val completion =
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                runtime.pump.request { reqId ->
                    KtBridge.workerComplete(runtime.handle, handle, kind.code, proto, reqId)
                }
            }
        if (completion.isFailure) {
            throw TemporalCoreException(
                message = "completing a ${kind.name.lowercase()} task failed: ${completion.errorMessage()}",
                errorType = null,
                statusCode = completion.status,
                cause = null,
                writableStackTrace = true,
            )
        }
    }

    /**
     * Records an activity heartbeat.
     *
     * Returns false rather than throwing when the worker is already shut down: a heartbeat racing
     * shutdown is expected, and it is the exact race that used to abort the JVM with SIGABRT when
     * the C bridge unwrapped a finalized worker.
     */
    fun heartbeat(proto: ByteArray) {
        val status = KtBridge.workerHeartbeat(handle, proto)
        if (status != KtBridge.KT_OK) {
            throw TemporalCoreException(
                message = "activity heartbeat rejected: ${KtBridge.lastError()}",
                errorType = null,
                statusCode = status,
                cause = null,
                writableStackTrace = true,
            )
        }
    }

    /**
     * Tells Core to stop handing out work, and returns immediately.
     *
     * Split from [shutdown] because a caller polling on another coroutine must be able to join
     * that poller, and the poll channels do not close until Core starts reporting `ShutDown`.
     * Calling only [shutdown] from a coroutine that is itself waiting on the poller deadlocks.
     */
    fun initiateShutdown() {
        if (closed) return
        try {
            KtBridge.workerInitiateShutdown(handle)
        } catch (e: KtBridgeException) {
            throw e.asCore("could not initiate worker shutdown")
        }
    }

    /**
     * Shuts down: stop accepting work, wait for all three streams to report `ShutDown`, finalize.
     */
    suspend fun shutdown(graceMillis: Long = 30_000L) {
        if (closed) return
        val completion =
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                runtime.pump.request { reqId ->
                    KtBridge.workerShutdown(runtime.handle, handle, graceMillis, reqId)
                }
            }
        closed = true
        if (completion.isFailure) {
            throw TemporalCoreException(
                message = "worker shutdown failed: ${completion.errorMessage()}",
                errorType = null,
                statusCode = completion.status,
                cause = null,
                writableStackTrace = true,
            )
        }
    }

    override fun close() {
        runtime.removeWorkerEvents(handle)
        KtBridge.workerFree(handle)
        workflowActivations.close()
        activityTasks.close()
        nexusTasks.close()
    }

    companion object {
        /** @param config an encoded `kt_bridge.WorkerOptions`. */
        fun create(
            runtime: KtRuntime,
            client: KtClient,
            config: ByteArray,
        ): KtWorker {
            runtime.ensureOpen()
            val handle =
                try {
                    KtBridge.workerNew(runtime.handle, client.handle, config)
                } catch (e: KtBridgeException) {
                    throw TemporalCoreException(
                        message = "could not create the worker: ${e.message}",
                        errorType = null,
                        statusCode = e.code,
                        cause = e,
                        writableStackTrace = true,
                    )
                }
            val worker = KtWorker(runtime, handle)
            // Registered before start(), so no task can arrive with nowhere to go.
            runtime.onWorkerEvents(handle, worker::onEvent)
            return worker
        }
    }
}
