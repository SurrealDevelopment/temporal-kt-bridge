package com.surrealdev.temporal.core

import com.google.protobuf.CodedInputStream
import com.google.protobuf.MessageLite
import com.surrealdev.temporal.core.kt.KtTaskKind
import com.surrealdev.temporal.core.kt.KtWorker
import kotlin.concurrent.write

/**
 * A high-level wrapper for a Temporal Core worker.
 *
 * Workers poll for tasks from the Temporal server and execute workflows and activities.
 * This class wraps the low-level FFM bindings and provides a coroutine-friendly API.
 *
 * Example usage:
 * ```kotlin
 * val worker = TemporalWorker.create(runtime, client, "my-task-queue", "default")
 * try {
 *     // Poll and complete tasks in a loop
 *     while (true) {
 *         val activation = worker.pollWorkflowActivation() ?: break
 *         // Process activation...
 *         worker.completeWorkflowActivation(completion)
 *     }
 * } finally {
 *     worker.initiateShutdown()
 *     worker.awaitShutdown()
 *     worker.close()
 * }
 * ```
 *
 * A Temporal worker.
 *
 * The polling API is unchanged -- [pollWorkflowActivation] still returns null when the worker
 * shuts down -- but Kotlin no longer drives Core's poll loops. Rust runs them and pushes tasks
 * onto channels that these methods receive from.
 *
 * That matters because Core does not finish shutting down until the language side has polled
 * every stream to `ShutDown`. Previously that contract lived in Kotlin, so cancelling a poll
 * coroutine could hang shutdown forever. Now cancelling a caller of these methods cannot affect
 * Core at all.
 */
class TemporalWorker private constructor(
    internal val kt: KtWorker,
    private val runtime: TemporalRuntime,
) : AutoCloseable {
    @Volatile
    private var closed = false

    @Volatile
    private var shutdownInitiated = false

    @Volatile
    private var shutdownFinalized = false

    companion object {
        /** Creates a worker and starts Core's poll loops. */
        fun create(
            runtime: TemporalRuntime,
            client: TemporalCoreClient,
            taskQueue: String,
            namespace: String,
            config: WorkerConfig = WorkerConfig(),
        ): TemporalWorker {
            runtime.ensureOpen()
            val worker =
                KtWorker.create(
                    runtime.kt,
                    client.kt,
                    WorkerOptionsProto.encode(taskQueue, namespace, config),
                )
            try {
                worker.start()
            } catch (t: Throwable) {
                worker.close()
                throw t
            }
            return TemporalWorker(worker, runtime)
        }
    }

    fun isClosed(): Boolean = closed

    fun isShutdownInitiated(): Boolean = shutdownInitiated

    fun isShutdownFinalized(): Boolean = shutdownFinalized

    /** No-op: Core validates the worker configuration when it is built. */
    suspend fun validate() {
        ensureOpen()
    }

    /** Returns the next workflow activation, or null once the stream has ended. */
    suspend fun <T : MessageLite> pollWorkflowActivation(parser: (CodedInputStream) -> T): T? =
        receive(kt.workflowActivationStream, parser)

    /** Returns the next activity task, or null once the stream has ended. */
    suspend fun <T : MessageLite> pollActivityTask(parser: (CodedInputStream) -> T): T? =
        receive(kt.activityTaskStream, parser)

    /** Returns the next nexus task, or null once the stream has ended. */
    suspend fun <T : MessageLite> pollNexusTask(parser: (CodedInputStream) -> T): T? =
        receive(kt.nexusTaskStream, parser)

    private suspend fun <T : MessageLite> receive(
        stream: kotlinx.coroutines.channels.ReceiveChannel<ByteArray>,
        parser: (CodedInputStream) -> T,
    ): T? =
        try {
            parser(CodedInputStream.newInstance(stream.receive()))
        } catch (_: kotlinx.coroutines.channels.ClosedReceiveChannelException) {
            // The stream reported ShutDown. Null is the documented end-of-stream signal.
            null
        }

    suspend fun <T : MessageLite> completeWorkflowActivation(completion: T) {
        ensureOpen()
        kt.complete(KtTaskKind.WORKFLOW_ACTIVATION, completion.toByteArray())
    }

    suspend fun <T : MessageLite> completeActivityTask(completion: T) {
        ensureOpen()
        kt.complete(KtTaskKind.ACTIVITY, completion.toByteArray())
    }

    suspend fun <T : MessageLite> completeNexusTask(completion: T) {
        ensureOpen()
        kt.complete(KtTaskKind.NEXUS, completion.toByteArray())
    }

    /**
     * Records an activity heartbeat.
     *
     * A heartbeat racing shutdown is expected -- a zombie activity thread outlives the worker --
     * and it must fail rather than be dropped, so the caller learns its worker is gone. This is
     * the race that used to abort the JVM with SIGABRT; the generation-counted handle turns the
     * use-after-free into [TemporalCoreException], and a closed wrapper into [IllegalStateException].
     */
    fun <T : MessageLite> recordActivityHeartbeat(heartbeat: T) {
        ensureOpen()
        kt.heartbeat(heartbeat.toByteArray())
    }

    /**
     * Asks Core to stop accepting work, and returns immediately.
     *
     * [awaitShutdown] does the waiting. The two are separate because Core's shutdown does not
     * complete until every poll stream has reported ShutDown, so a caller with an in-flight
     * poller has to be able to end the polling before it joins it.
     */
    fun initiateShutdown() {
        if (closed || shutdownFinalized) return
        shutdownInitiated = true
        kt.initiateShutdown()
    }

    /** Waits for every poll stream to end and the worker to finalize. */
    suspend fun awaitShutdown() {
        if (shutdownFinalized) return
        shutdownInitiated = true
        kt.shutdown()
        shutdownFinalized = true
    }

    private fun ensureOpen() {
        check(!closed) { "Worker has been closed" }
        runtime.ensureOpen()
    }

    override fun close() {
        if (closed) return
        synchronized(this) {
            if (closed) return
            closed = true
            kt.close()
        }
    }
}

private fun SlotSupplier.fixedSlotsOrZero(): Int = (this as? SlotSupplier.FixedSize)?.slots ?: 0

private class ProtoBuf {
    private val out = java.io.ByteArrayOutputStream()

    private fun varint(value: Long) {
        var v = value
        while (v and 0x7FL.inv() != 0L) {
            out.write(((v and 0x7F) or 0x80).toInt())
            v = v ushr 7
        }
        out.write(v.toInt())
    }

    fun double(
        number: Int,
        value: Double,
    ) = apply {
        out.write((number shl 3) or 1)
        var bits = java.lang.Double.doubleToLongBits(value)
        repeat(8) {
            out.write((bits and 0xFF).toInt())
            bits = bits ushr 8
        }
    }

    fun uint(
        number: Int,
        value: Long,
    ) = apply {
        if (value > 0) {
            out.write((number shl 3) or 0)
            varint(value)
        }
    }

    fun bytes(): ByteArray = out.toByteArray()
}

/**
 * The shared PID targets and gains, or null when this supplier is not resource-based.
 *
 * Fixed-width doubles rather than varints: a gain of 0.0 is meaningful (it is `i_gain`'s default),
 * so these fields cannot be skipped the way a zero-valued varint is.
 */
@Suppress("DEPRECATION")
private fun SlotSupplier.resourceTargets(): ByteArray? =
    when (this) {
        is SlotSupplier.FixedSize -> null
        is SlotSupplier.JvmResourceBased ->
            ProtoBuf()
                .double(1, targetMemoryUsage)
                .double(2, targetCpuUsage)
                .double(3, pidTuning.memoryPGain)
                .double(4, pidTuning.memoryIGain)
                .double(5, pidTuning.memoryDGain)
                .double(6, pidTuning.memoryOutputThreshold)
                .double(7, pidTuning.cpuPGain)
                .double(8, pidTuning.cpuIGain)
                .double(9, pidTuning.cpuDGain)
                .double(10, pidTuning.cpuOutputThreshold)
                .bytes()
        // CGroupResourceBased carried no PID knobs, so Core's defaults apply. The JVM-heap vs
        // system-memory distinction the two variants drew disappears here: Core samples the
        // system (cgroup-aware) either way, which is why JvmResourceBased is the survivor.
        is SlotSupplier.CGroupResourceBased ->
            ProtoBuf()
                .double(1, targetMemoryUsage)
                .double(2, targetCpuUsage)
                .double(3, 5.0)
                .double(4, 0.0)
                .double(5, 1.0)
                .double(6, 0.25)
                .double(7, 5.0)
                .double(8, 0.0)
                .double(9, 1.0)
                .double(10, 0.05)
                .bytes()
    }

/** This slot type's own bounds, or null when it is fixed-size. */
@Suppress("DEPRECATION")
private fun SlotSupplier.resourceLimits(): ByteArray? =
    when (this) {
        is SlotSupplier.FixedSize -> null
        is SlotSupplier.JvmResourceBased ->
            ProtoBuf()
                .uint(1, minimumSlots.toLong())
                .uint(2, maximumSlots.toLong())
                .uint(3, rampThrottleMs)
                .bytes()
        is SlotSupplier.CGroupResourceBased ->
            ProtoBuf()
                .uint(1, minimumSlots.toLong())
                .uint(2, maximumSlots.toLong())
                .uint(3, rampThrottleMs)
                .bytes()
    }

/** Encodes `kt_bridge.WorkerOptions` by hand: the bridge's own config protos are not published. */
internal object WorkerOptionsProto {
    fun encode(
        taskQueue: String,
        namespace: String,
        config: WorkerConfig,
    ): ByteArray {
        val out = java.io.ByteArrayOutputStream()

        fun varint(value: Int) {
            var v = value
            while (v >= 0x80) {
                out.write((v and 0x7F) or 0x80)
                v = v ushr 7
            }
            out.write(v)
        }

        fun string(
            number: Int,
            value: String,
        ) {
            if (value.isEmpty()) return
            val bytes = value.toByteArray(Charsets.UTF_8)
            out.write((number shl 3) or 2)
            varint(bytes.size)
            out.write(bytes)
        }

        fun int(
            number: Int,
            value: Int,
        ) {
            if (value <= 0) return
            out.write((number shl 3) or 0)
            varint(value)
        }

        fun bool(
            number: Int,
            value: Boolean,
        ) {
            if (!value) return
            out.write((number shl 3) or 0)
            out.write(1)
        }

        fun message(
            number: Int,
            body: ByteArray,
        ) {
            out.write((number shl 3) or 2)
            varint(body.size)
            out.write(body)
        }
        string(1, namespace)
        string(2, taskQueue)
        // Empty means the bridge derives <pid>@<hostname>, so this is the worker's override only.
        string(3, config.workerIdentity ?: "")
        int(4, config.maxCachedWorkflows)
        // Versioning itself is not wired through yet; the build id is still worth sending so
        // history shows which build ran a task.
        string(9, config.deploymentOptions?.version?.buildId ?: "")

        // Negated on the wire so proto3's false default means "enabled".
        bool(8, !config.enableActivities)
        bool(14, !config.enableWorkflows)
        bool(15, !config.enableLocalActivities)

        // Fixed slot counts. A resource-based supplier leaves these at 0 and sends limits below.
        int(5, config.workflowSlotSupplier.fixedSlotsOrZero())
        int(6, config.activitySlotSupplier.fixedSlotsOrZero())
        int(7, config.localActivitySlotSupplier.fixedSlotsOrZero())

        // Resource-based tuning is Core's own supplier now. The previous bridge ran the identical
        // algorithm in the JVM -- same PID gains, same defaults -- behind seven FFM upcalls sitting
        // in front of every poll, so moving it into Rust costs nothing but the upcalls.
        //
        // Core shares one controller across a worker's slot types, so the targets and gains are
        // taken from whichever supplier is resource-based (they are the same object in every
        // realistic configuration) while each slot type sends its own limits. A slot type left
        // fixed sends none and stays fixed.
        val suppliers =
            listOf(
                11 to config.workflowSlotSupplier,
                12 to config.activitySlotSupplier,
                13 to config.localActivitySlotSupplier,
            )
        suppliers.firstNotNullOfOrNull { (_, s) -> s.resourceTargets() }?.let { message(10, it) }
        suppliers.forEach { (field, supplier) -> supplier.resourceLimits()?.let { message(field, it) } }
        return out.toByteArray()
    }
}
