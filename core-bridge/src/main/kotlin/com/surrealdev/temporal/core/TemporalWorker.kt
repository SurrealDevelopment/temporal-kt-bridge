package com.surrealdev.temporal.core

import com.google.protobuf.CodedInputStream
import com.google.protobuf.MessageLite
import com.surrealdev.temporal.core.kt.KtTaskKind
import com.surrealdev.temporal.core.kt.KtWorker

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
        /** Creates and validates a worker before starting Core's poll loops. Network validation is cancellable. */
        suspend fun create(
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
                runtime.registerResourceWorker(worker.handle, config)
                worker.start()
            } catch (t: Throwable) {
                runtime.removeResourceWorker(worker.handle)
                worker.close()
                throw t
            }
            return TemporalWorker(worker, runtime)
        }
    }

    /** Observes JVM tuning decisions and slot counts on the runtime sampler thread. */
    fun onSlotSupplierMetrics(callback: ((SlotSupplierMetrics) -> Unit)?) {
        runtime.onResourceMetrics(kt.handle, callback)
    }

    fun isClosed(): Boolean = closed || runtime.isClosed()

    fun isShutdownInitiated(): Boolean = shutdownInitiated

    fun isShutdownFinalized(): Boolean = shutdownFinalized

    /** No-op: [create] already completed Core's configuration and server-side validation. */
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
        stream: com.surrealdev.temporal.core.kt.TaskStream,
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
            runtime.removeResourceWorker(kt.handle)
            kt.close()
        }
    }
}

/** The generated schema is shared with Rust, including presence for meaningful zero values. */
internal object WorkerOptionsProto {
    fun encode(
        taskQueue: String,
        namespace: String,
        config: WorkerConfig,
    ): ByteArray {
        require(taskQueue.isNotBlank()) { "taskQueue must not be blank" }
        require(namespace.isNotBlank()) { "namespace must not be blank" }
        require(config.maxCachedWorkflows >= 0) { "maxCachedWorkflows must be nonnegative" }
        require(config.enableWorkflows || !config.enableLocalActivities) {
            "Local activities require workflows to be enabled"
        }
        require(config.enableWorkflows || config.enableActivities || config.enableNexus) {
            "At least one task type must be enabled"
        }
        require(config.maxHeartbeatThrottleIntervalMs >= 0) { "maxHeartbeatThrottleIntervalMs must be nonnegative" }
        require(
            config.defaultHeartbeatThrottleIntervalMs >= 0,
        ) { "defaultHeartbeatThrottleIntervalMs must be nonnegative" }
        require(
            config.stickyQueueScheduleToStartTimeoutMs >= 0,
        ) { "stickyQueueScheduleToStartTimeoutMs must be nonnegative" }
        require((config.gracefulShutdownPeriodMs ?: 0) >= 0) {
            "gracefulShutdownPeriodMs must be nonnegative"
        }
        require(config.maxActivitiesPerSecond.isFinite() && config.maxActivitiesPerSecond >= 0) {
            "maxActivitiesPerSecond must be finite and nonnegative"
        }
        require(config.maxTaskQueueActivitiesPerSecond.isFinite() && config.maxTaskQueueActivitiesPerSecond >= 0) {
            "maxTaskQueueActivitiesPerSecond must be finite and nonnegative"
        }
        require(config.nonstickyToStickyPollRatio.isFinite() && config.nonstickyToStickyPollRatio in 0f..1f) {
            "nonstickyToStickyPollRatio must be between 0 and 1"
        }
        require(config.maxEagerActivityReservationsPerWorkflowTask >= 0) {
            "maxEagerActivityReservationsPerWorkflowTask must be nonnegative"
        }
        require(config.nondeterminismAsWorkflowFailForTypes.all { it.isNotBlank() }) {
            "nondeterminismAsWorkflowFailForTypes must contain nonblank workflow type names"
        }
        val options =
            com.surrealdev.temporal.core.proto.WorkerOptions
                .newBuilder()
                .setNamespace(namespace)
                .setTaskQueue(taskQueue)
                .setIdentity(config.workerIdentity.orEmpty())
                .setMaxCachedWorkflows(config.maxCachedWorkflows)
                .setBuildId(config.buildId)
                .setNoRemoteActivities(!config.enableActivities)
                .setNoWorkflows(!config.enableWorkflows)
                .setNoLocalActivities(!config.enableLocalActivities)
                .setEnableNexus(config.enableNexus)
                .setMaxHeartbeatThrottleIntervalMillis(config.maxHeartbeatThrottleIntervalMs)
                .setDefaultHeartbeatThrottleIntervalMillis(config.defaultHeartbeatThrottleIntervalMs)
                .setMaxActivitiesPerSecond(config.maxActivitiesPerSecond)
                .setMaxTaskQueueActivitiesPerSecond(config.maxTaskQueueActivitiesPerSecond)
                .setNonstickyToStickyPollRatio(config.nonstickyToStickyPollRatio)
                .setStickyQueueScheduleToStartTimeoutMillis(config.stickyQueueScheduleToStartTimeoutMs)
                .setNondeterminismAsWorkflowFail(config.nondeterminismAsWorkflowFail)
                .addAllNondeterminismAsWorkflowFailForTypes(config.nondeterminismAsWorkflowFailForTypes)
                .setMaxEagerActivityReservationsPerWorkflowTask(config.maxEagerActivityReservationsPerWorkflowTask)
                .setDisablePayloadErrorLimit(config.disablePayloadErrorLimit)

        config.gracefulShutdownPeriodMs?.let(options::setGracefulShutdownPeriodMillis)
        config.deploymentOptions?.let { deployment ->
            require(
                deployment.useWorkerVersioning ||
                    deployment.defaultVersioningBehavior == VersioningBehavior.UNSPECIFIED,
            ) {
                "defaultVersioningBehavior requires useWorkerVersioning"
            }
            options.setDeploymentOptions(
                com.surrealdev.temporal.core.proto.WorkerDeploymentOptions
                    .newBuilder()
                    .setDeploymentName(deployment.version.deploymentName)
                    .setBuildId(deployment.version.buildId)
                    .setUseWorkerVersioning(deployment.useWorkerVersioning)
                    .setDefaultVersioningBehavior(deployment.defaultVersioningBehavior.value),
            )
        }
        config.workflowPollerBehavior?.let {
            if (it is CorePollerBehavior.SimpleMaximum && config.maxCachedWorkflows > 0) {
                require(it.maximum >= 2) { "Cached workflows require at least 2 workflow pollers" }
            }
            options.setWorkflowPollerBehavior(it.toProto())
        }
        config.activityPollerBehavior?.let { options.setActivityPollerBehavior(it.toProto()) }
        config.nexusPollerBehavior?.let { options.setNexusPollerBehavior(it.toProto()) }

        var sharedTargets: com.surrealdev.temporal.core.proto.ResourceBasedTuner? = null

        fun supplier(
            supplier: SlotSupplier,
            setFixed: (Int) -> Unit,
            setResource: (com.surrealdev.temporal.core.proto.ResourceSlotLimits) -> Unit,
        ) {
            if (supplier is SlotSupplier.FixedSize) {
                require(supplier.slots > 0) { "Fixed slot counts must be positive" }
                setFixed(supplier.slots)
                return
            }
            val targets = supplier.resourceTargets()
            require(sharedTargets == null || sharedTargets?.jvmResourceBased == targets.jvmResourceBased) {
                "JVM and system resource suppliers cannot be mixed in one worker"
            }
            require(targets.jvmResourceBased || sharedTargets == null || sharedTargets == targets) {
                "System resource suppliers in one worker must share memory/CPU targets and PID tuning"
            }
            sharedTargets = targets
            setResource(supplier.resourceLimits())
        }
        if (config.maxCachedWorkflows > 0 && config.enableWorkflows) {
            require(config.workflowSlotSupplier.maxConcurrent >= 2) {
                "Cached workflows require at least 2 workflow slots"
            }
        }
        supplier(config.workflowSlotSupplier, {
            options.setMaxConcurrentWorkflowTasks(it)
        }, { options.setWorkflowResourceLimits(it) })
        supplier(config.activitySlotSupplier, {
            options.setMaxConcurrentActivities(it)
        }, { options.setActivityResourceLimits(it) })
        supplier(config.localActivitySlotSupplier, {
            options.setMaxConcurrentLocalActivities(it)
        }, { options.setLocalActivityResourceLimits(it) })
        supplier(
            config.nexusSlotSupplier,
            { options.setMaxConcurrentNexusTasks(it) },
            { options.setNexusResourceLimits(it) },
        )
        sharedTargets?.let(options::setResourceTuner)
        return options.build().toByteArray()
    }
}

private fun CorePollerBehavior.toProto(): com.surrealdev.temporal.core.proto.PollerBehavior {
    val builder =
        com.surrealdev.temporal.core.proto.PollerBehavior
            .newBuilder()
    when (this) {
        is CorePollerBehavior.SimpleMaximum -> {
            require(maximum > 0) { "SimpleMaximum poller maximum must be positive" }
            builder.setSimpleMaximum(maximum)
        }
        is CorePollerBehavior.Autoscaling -> {
            require(minimum > 0 && maximum >= minimum && initial in minimum..maximum) {
                "Autoscaling requires 0 < minimum <= initial <= maximum"
            }
            builder.setAutoscaling(
                com.surrealdev.temporal.core.proto.PollerAutoscaling
                    .newBuilder()
                    .setMinimum(minimum)
                    .setMaximum(maximum)
                    .setInitial(initial),
            )
        }
    }
    return builder.build()
}

@Suppress("DEPRECATION")
private fun SlotSupplier.asResource(): SlotSupplier.JvmResourceBased =
    when (this) {
        is SlotSupplier.JvmResourceBased -> this
        is SlotSupplier.CGroupResourceBased ->
            SlotSupplier.JvmResourceBased(targetMemoryUsage, targetCpuUsage, minimumSlots, maximumSlots, rampThrottleMs)
        is SlotSupplier.FixedSize -> error("Fixed suppliers do not have resource options")
    }

private fun SlotSupplier.resourceTargets(): com.surrealdev.temporal.core.proto.ResourceBasedTuner {
    val resource = asResource()
    require(resource.targetMemoryUsage.isFinite() && resource.targetMemoryUsage in 0.0..1.0) {
        "targetMemoryUsage must be between 0 and 1"
    }
    require(resource.targetCpuUsage.isFinite() && resource.targetCpuUsage in 0.0..1.0) {
        "targetCpuUsage must be between 0 and 1"
    }
    with(resource.pidTuning) {
        require(
            listOf(
                memoryPGain,
                memoryIGain,
                memoryDGain,
                memoryOutputThreshold,
                cpuPGain,
                cpuIGain,
                cpuDGain,
                cpuOutputThreshold,
            ).all { it.isFinite() },
        ) { "Resource PID gains and thresholds must be finite" }
        return com.surrealdev.temporal.core.proto.ResourceBasedTuner
            .newBuilder()
            .setJvmResourceBased(this@resourceTargets is SlotSupplier.JvmResourceBased)
            .setTargetMemoryUsage(resource.targetMemoryUsage)
            .setTargetCpuUsage(resource.targetCpuUsage)
            .setMemoryPGain(memoryPGain)
            .setMemoryIGain(memoryIGain)
            .setMemoryDGain(memoryDGain)
            .setMemoryOutputThreshold(memoryOutputThreshold)
            .setCpuPGain(cpuPGain)
            .setCpuIGain(cpuIGain)
            .setCpuDGain(cpuDGain)
            .setCpuOutputThreshold(cpuOutputThreshold)
            .build()
    }
}

private fun SlotSupplier.resourceLimits(): com.surrealdev.temporal.core.proto.ResourceSlotLimits {
    val resource = asResource()
    require(resource.minimumSlots >= 0 && resource.maximumSlots > 0 && resource.minimumSlots <= resource.maximumSlots) {
        "Resource slots require 0 <= minimumSlots <= maximumSlots and maximumSlots > 0"
    }
    require(resource.rampThrottleMs >= 0) { "Resource rampThrottleMs must be nonnegative" }
    return com.surrealdev.temporal.core.proto.ResourceSlotLimits
        .newBuilder()
        .setMinimumSlots(resource.minimumSlots)
        .setMaximumSlots(resource.maximumSlots)
        .setRampThrottleMillis(resource.rampThrottleMs)
        .build()
}
