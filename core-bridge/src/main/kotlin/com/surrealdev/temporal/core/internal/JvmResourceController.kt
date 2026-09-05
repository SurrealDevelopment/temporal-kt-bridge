package com.surrealdev.temporal.core.internal

import com.surrealdev.temporal.core.SlotSupplier

/** One controller per slot type: changing activity tuning must not retune workflow slots. */
internal class JvmResourceController(
    private val options: SlotSupplier.JvmResourceBased,
) {
    private val memory =
        with(options.pidTuning) {
            PidController(options.targetMemoryUsage, memoryPGain, memoryIGain, memoryDGain)
        }
    private val cpu =
        with(options.pidTuning) {
            PidController(options.targetCpuUsage, cpuPGain, cpuIGain, cpuDGain)
        }
    private var lastUpdate: Long? = null
    var memoryOutput: Double = 0.0
        private set
    var cpuOutput: Double = 0.0
        private set
    var allow: Boolean = false
        private set

    fun update(
        memoryUsage: Double,
        cpuLoad: Double,
        nowNanos: Long = System.nanoTime(),
    ): Boolean {
        val previous = lastUpdate
        if (previous != null && (nowNanos - previous) / 1_000_000 < options.rampThrottleMs) {
            allow = allow && memoryUsage < options.targetMemoryUsage
            return allow
        }
        lastUpdate = nowNanos
        memoryOutput = memory.update(memoryUsage)
        cpuOutput = cpu.update(cpuLoad)
        // A hard memory cap prevents integral history granting through a heap spike.
        allow = memoryUsage < options.targetMemoryUsage &&
            memoryOutput > options.pidTuning.memoryOutputThreshold &&
            cpuOutput > options.pidTuning.cpuOutputThreshold
        return allow
    }
}
