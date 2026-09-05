package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.util.concurrent.ConcurrentLinkedQueue

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

/** Cancellation only consumes a wakeup; a task stays queued until receive actually returns. */
internal class TaskStream {
    private val tasks = ConcurrentLinkedQueue<ByteArray>()
    private val ready = Channel<Unit>(Channel.CONFLATED)

    @Volatile
    private var closed = false

    fun send(task: ByteArray) {
        if (closed) return
        tasks.add(task)
        ready.trySend(Unit)
    }

    fun close(cause: Throwable? = null) {
        closed = true
        ready.close(cause)
    }

    suspend fun receive(): ByteArray {
        while (true) {
            currentCoroutineContext().ensureActive()
            tasks.poll()?.let { task ->
                // Another receiver may already be waiting after observing the queue empty.
                if (tasks.isNotEmpty()) ready.trySend(Unit)
                return task
            }
            val wakeup =
                try {
                    ready.receiveCatching()
                } finally {
                    // Prompt cancellation can consume a wakeup without consuming its task.
                    if (tasks.isNotEmpty()) ready.trySend(Unit)
                }
            if (wakeup.isClosed) {
                // A final task can be enqueued between the empty check and stream closure.
                tasks.poll()?.let { return it }
                throw wakeup.exceptionOrNull() ?: ClosedReceiveChannelException("task stream closed")
            }
        }
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
    private val workflowActivations = TaskStream()
    private val activityTasks = TaskStream()
    private val nexusTasks = TaskStream()
    private val streamFailures = arrayOfNulls<TemporalCoreException>(KtTaskKind.entries.size)

    @Volatile
    private var closed = false

    val workflowActivationStream: TaskStream get() = workflowActivations
    val activityTaskStream: TaskStream get() = activityTasks
    val nexusTaskStream: TaskStream get() = nexusTasks

    private fun onEvent(completion: Completion) {
        when (completion.kind) {
            Kind.TASK_WORKFLOW_ACTIVATION -> workflowActivations.send(completion.payload)
            Kind.TASK_ACTIVITY -> activityTasks.send(completion.payload)
            Kind.TASK_NEXUS -> nexusTasks.send(completion.payload)
            Kind.TASK_STREAM_END ->
                KtTaskKind.fromAux(completion.aux1)?.let { kind ->
                    channelFor(completion.aux1)?.close(streamFailures[kind.code])
                }
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
                // Keep accepting shutdown activations/tasks until the stream ends. Dropping them
                // would strand the Core slots that shutdown is waiting to release.
                KtTaskKind.fromAux(completion.aux1)?.let { streamFailures[it.code] = failure }
            }
            else -> Unit
        }
    }

    private fun channelFor(aux1: Long): TaskStream? =
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
        if (completion.isFailure) {
            throw TemporalCoreException(
                message = "worker shutdown failed: ${completion.errorMessage()}",
                errorType = null,
                statusCode = completion.status,
                cause = null,
                writableStackTrace = true,
            )
        }
        closed = true
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
            try {
                runtime.onWorkerEvents(handle, worker::onEvent)
            } catch (t: Throwable) {
                worker.close()
                throw t
            }
            return worker
        }
    }
}
