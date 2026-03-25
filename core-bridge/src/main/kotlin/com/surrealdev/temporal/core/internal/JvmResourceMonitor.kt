package com.surrealdev.temporal.core.internal

import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * Samples JVM heap and per-process CPU metrics on a background thread.
 *
 * Scoped to the application lifecycle — created when the application starts,
 * stopped when it closes. Shared across all workers in the same application.
 *
 * @param executor Shared scheduled executor for the sampling task
 */
class JvmResourceMonitor(
    private val executor: ScheduledExecutorService,
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(JvmResourceMonitor::class.java)

    @Volatile
    private var memoryUsageValue: Double = 0.0

    @Volatile
    private var cpuLoadValue: Double = 0.0

    /** Old gen memory pool, or null if not found. */
    private val oldGenPool: MemoryPoolMXBean? = findOldGenPool()

    /** Process CPU bean, or null if unavailable on this JVM. */
    private val osMxBean: com.sun.management.OperatingSystemMXBean? = resolveOsMxBean()

    private val heapMxBean = ManagementFactory.getMemoryMXBean()

    private val sampleTask: ScheduledFuture<*>

    init {
        sampleTask =
            executor.scheduleAtFixedRate(
                ::sample,
                0L,
                SAMPLE_INTERVAL_MS,
                TimeUnit.MILLISECONDS,
            )

        if (oldGenPool != null) {
            logger.debug("JvmResourceMonitor using old gen pool: {}", oldGenPool.name)
        } else {
            logger.debug("JvmResourceMonitor: no old gen pool found, using total heap usage")
        }
        if (osMxBean == null) {
            logger.warn(
                "JvmResourceMonitor: OperatingSystemMXBean not available, CPU throttling disabled",
            )
        }
    }

    /**
     * Returns the current JVM old gen memory usage ratio (0.0–1.0).
     *
     * Uses real-time `getUsage()` for immediate spike detection. Falls back to total heap
     * if old gen pool is not found.
     */
    fun memoryUsage(): Double = memoryUsageValue

    /**
     * Returns the current per-process CPU load (0.0–1.0).
     *
     * Returns 0.0 if CPU monitoring is unavailable.
     */
    fun cpuLoad(): Double = cpuLoadValue

    override fun close() {
        sampleTask.cancel(false)
        executor.shutdown()
    }

    private fun sample() {
        try {
            memoryUsageValue = sampleMemory()
            cpuLoadValue = sampleCpu()
        } catch (e: Exception) {
            logger.debug("JvmResourceMonitor sample failed", e)
        }
    }

    private fun sampleMemory(): Double {
        // Use old gen getUsage() for real-time visibility into memory pressure.
        // Unlike getCollectionUsage() (post-GC only), this updates continuously and
        // lets the PID controller react immediately to spikes. The PID's derivative
        // term smooths the sawtooth noise from young gen GC.
        val pool = oldGenPool
        if (pool != null) {
            val usage = pool.usage
            if (usage != null && usage.max > 0) {
                return usage.used.toDouble() / usage.max.toDouble()
            }
        }

        // Fallback: total heap usage (if old gen pool not found)
        val heap = heapMxBean.heapMemoryUsage
        if (heap.max > 0) {
            return heap.used.toDouble() / heap.max.toDouble()
        }
        return 0.0
    }

    private fun sampleCpu(): Double {
        val bean = osMxBean ?: return 0.0
        val load = bean.cpuLoad
        // getCpuLoad() returns -1.0 when data is unavailable (e.g., first call)
        if (load < 0.0) return 0.0
        return load.coerceIn(0.0, 1.0)
    }

    companion object {
        private const val SAMPLE_INTERVAL_MS = 100L

        private fun findOldGenPool(): MemoryPoolMXBean? {
            val oldGenNames =
                setOf(
                    "Old Gen",
                    "Tenured Gen",
                    "PS Old Gen",
                    "G1 Old Gen",
                    "ZHeap",
                    "Shenandoah",
                )
            return ManagementFactory.getMemoryPoolMXBeans().firstOrNull { pool ->
                oldGenNames.any { pool.name.contains(it, ignoreCase = true) }
            }
        }

        private fun resolveOsMxBean(): com.sun.management.OperatingSystemMXBean? =
            try {
                ManagementFactory.getOperatingSystemMXBean() as? com.sun.management.OperatingSystemMXBean
            } catch (_: Exception) {
                null
            }
    }
}
