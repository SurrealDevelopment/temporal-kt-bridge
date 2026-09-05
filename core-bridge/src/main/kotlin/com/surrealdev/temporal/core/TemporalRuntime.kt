package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.CoreMetricsBridge
import com.surrealdev.temporal.core.internal.JvmResourceController
import com.surrealdev.temporal.core.internal.JvmResourceMonitor
import com.surrealdev.temporal.core.kt.KtBridge
import com.surrealdev.temporal.core.kt.KtRuntime
import com.surrealdev.temporal.core.proto.MetricBatch
import com.surrealdev.temporal.core.proto.RuntimeOptions
import com.surrealdev.temporal.core.proto.TelemetryOptions
import io.opentelemetry.api.metrics.Meter
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Owns Core, its completion pump, and the JVM resource/telemetry sampler. */
class TemporalRuntime private constructor(
    internal val kt: KtRuntime,
    private val metrics: CoreMetricsBridge?,
) : AutoCloseable {
    @Volatile
    private var closed = false
    private val logger = LoggerFactory.getLogger(TemporalRuntime::class.java)
    private val sampler =
        Executors.newSingleThreadScheduledExecutor(
            Thread
                .ofPlatform()
                .daemon(true)
                .name("temporal-runtime-sampler")
                .factory(),
        )
    private var monitor: JvmResourceMonitor? = null
    private val sampleLock = Any()
    private val resourceWorkers = mutableMapOf<Long, ResourceWorker>()

    private class ResourceWorker(
        val controllers: List<JvmResourceController?>,
        var callback: ((SlotSupplierMetrics) -> Unit)? = null,
    )

    init {
        sampler.scheduleWithFixedDelay({ sample() }, 50, 50, TimeUnit.MILLISECONDS)
    }

    companion object {
        fun create(): TemporalRuntime = create(coreMetricsMeter = null)

        /**
         * Replays Core metrics into [coreMetricsMeter], an OpenTelemetry Meter supplied by the
         * application. Worker heartbeats accept 1000–60000 ms, or zero to disable them.
         */
        fun create(
            coreMetricsMeter: Any?,
            workerHeartbeatIntervalMs: Long = 60_000L,
        ): TemporalRuntime {
            require(workerHeartbeatIntervalMs == 0L || workerHeartbeatIntervalMs in 1000L..60_000L) {
                "workerHeartbeatIntervalMs must be zero or between 1000 and 60000"
            }
            val metrics =
                coreMetricsMeter?.let {
                    require(it is Meter) { "coreMetricsMeter must be an OpenTelemetry Meter" }
                    CoreMetricsBridge(it)
                }
            val config =
                RuntimeOptions
                    .newBuilder()
                    .setWorkerHeartbeatIntervalMillis(workerHeartbeatIntervalMs)
                    .setTelemetry(
                        TelemetryOptions
                            .newBuilder()
                            .setForwardLogs(true)
                            .setLogFilter("WARN")
                            .setBufferMetrics(metrics != null),
                    ).build()
            val kt = KtRuntime.create(config.toByteArray())
            return try {
                TemporalRuntime(kt, metrics)
            } catch (t: Throwable) {
                kt.close()
                throw t
            }
        }
    }

    internal fun registerResourceWorker(
        handle: Long,
        config: WorkerConfig,
    ) = synchronized(this) {
        ensureOpen()
        val controllers =
            listOf(
                config.workflowSlotSupplier.takeIf { config.enableWorkflows },
                config.activitySlotSupplier.takeIf { config.enableActivities },
                config.localActivitySlotSupplier.takeIf { config.enableLocalActivities },
                config.nexusSlotSupplier.takeIf { config.enableNexus },
            ).map { (it as? SlotSupplier.JvmResourceBased)?.let(::JvmResourceController) }
        if (controllers.any { it != null }) {
            if (monitor == null) monitor = JvmResourceMonitor(sampler)
            val worker = ResourceWorker(controllers)
            updateResource(handle, worker)
            resourceWorkers[handle] = worker
        }
    }

    internal fun onResourceMetrics(
        handle: Long,
        callback: ((SlotSupplierMetrics) -> Unit)?,
    ) = synchronized(this) {
        resourceWorkers[handle]?.callback = callback
    }

    internal fun removeResourceWorker(handle: Long) =
        synchronized(this) {
            resourceWorkers.remove(handle)
            Unit
        }

    private fun updateResource(
        handle: Long,
        worker: ResourceWorker,
    ): List<SlotSupplierMetrics> {
        val memory = monitor!!.memoryUsage()
        val cpu = monitor!!.cpuLoad()
        var mask = 0
        worker.controllers.forEachIndexed { kind, controller ->
            if (controller?.update(memory, cpu) == true) mask = mask or (1 shl kind)
        }
        val stats = KtBridge.updateResource(handle, mask)
        val names = listOf("workflow", "activity", "local_activity", "nexus")
        return worker.controllers.mapIndexedNotNull { kind, controller ->
            controller?.let {
                SlotSupplierMetrics(
                    names[kind],
                    memory,
                    cpu,
                    it.memoryOutput,
                    it.cpuOutput,
                    stats[kind * 2],
                    stats[kind * 2 + 1],
                )
            }
        }
    }

    private fun sample() =
        synchronized(sampleLock) {
            if (closed) return@synchronized
            val deliveries =
                synchronized(this) {
                    resourceWorkers.toList().mapNotNull { (handle, worker) ->
                        try {
                            val samples = updateResource(handle, worker)
                            worker.callback?.let { it to samples }
                        } catch (e: Exception) {
                            logger.warn("Could not sample worker resource usage", e)
                            null
                        }
                    }
                }
            // User hooks may close workers. Run them without the runtime's worker-registration lock.
            deliveries.forEach { (callback, samples) ->
                samples.forEach { sample ->
                    if (closed) return@synchronized
                    try {
                        callback(sample)
                    } catch (e: Exception) {
                        logger.warn("Slot supplier metrics callback failed", e)
                    }
                }
            }
            if (!closed) drainTelemetry()
        }

    private fun drainTelemetry() {
        try {
            val batch = MetricBatch.parseFrom(KtBridge.retrieveMetrics(kt.handle))
            batch.logsList.forEach { log ->
                val target = LoggerFactory.getLogger(log.target.ifEmpty { "temporal.core" })
                val message = if (log.fieldsJson.isEmpty()) log.message else "${log.message} ${log.fieldsJson}"
                when (log.level) {
                    "ERROR" -> target.error(message)
                    "WARN" -> target.warn(message)
                    "INFO" -> target.info(message)
                    "DEBUG" -> target.debug(message)
                    else -> target.trace(message)
                }
            }
            metrics?.record(batch)
        } catch (e: Exception) {
            logger.warn("Could not collect Core telemetry", e)
        }
    }

    fun isClosed(): Boolean = closed

    override fun close() {
        if (closed) return
        synchronized(sampleLock) {
            if (closed) return@synchronized
            synchronized(this) {
                closed = true
                sampler.shutdown()
                monitor?.close()
                resourceWorkers.clear()
            }
            try {
                drainTelemetry()
            } finally {
                try {
                    kt.close()
                } finally {
                    EphemeralServers.unregisterClosed()
                }
            }
        }
    }

    internal fun ensureOpen() {
        check(!closed) { "Runtime has been closed" }
    }
}

/** A JVM resource sample and the native slot counts for one task type. */
data class SlotSupplierMetrics(
    val slotType: String,
    val memoryUsage: Double,
    val cpuLoad: Double,
    val memoryPidOutput: Double,
    val cpuPidOutput: Double,
    val activeSlots: Int,
    val pendingReserves: Int,
)
