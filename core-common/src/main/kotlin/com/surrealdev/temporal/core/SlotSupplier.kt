package com.surrealdev.temporal.core

/**
 * Controls how a worker manages execution slots for a particular task type
 * (workflows, activities, local activities, or nexus tasks).
 */
sealed class SlotSupplier {
    /**
     * Uses a fixed number of execution slots. A simple concurrency limit.
     *
     * @param slots Maximum number of concurrent executions
     */
    data class FixedSize(
        val slots: Int,
    ) : SlotSupplier()

    /**
     * Dynamically adjusts execution slots based on JVM heap and per-process CPU usage.
     *
     * @param targetMemoryUsage Target JVM old gen usage (0.0 to 1.0). Slots are denied when above this.
     * @param targetCpuUsage Target per-process CPU usage (0.0 to 1.0). Slots are denied when above this.
     * @param minimumSlots Minimum slots always granted regardless of resource pressure.
     * @param maximumSlots Hard upper bound on slot count.
     * @param rampThrottleMs Interval (ms) between grant decisions, to prevent burst allocation.
     */
    data class JvmResourceBased(
        val targetMemoryUsage: Double = 0.8,
        val targetCpuUsage: Double = 0.8,
        val minimumSlots: Int = 1,
        val maximumSlots: Int = 10000,
        val rampThrottleMs: Long = 50,
        val pidTuning: PidTuning = PidTuning(),
    ) : SlotSupplier() {
        /**
         * PID controller tuning parameters for resource-based slot decisions.
         *
         * The PID controller outputs a signal based on how far current resource usage
         * is from the target. A positive output above the threshold means resources are
         * available and slots can be granted. Defaults match the Temporal Core SDK.
         *
         * Most users should not need to change these. Tuning is only necessary for
         * workloads with unusual resource consumption patterns.
         *
         * @param memoryPGain Proportional gain for memory — reacts to current error magnitude
         * @param memoryIGain Integral gain for memory — reacts to accumulated error over time
         * @param memoryDGain Derivative gain for memory — reacts to rate of error change
         * @param memoryOutputThreshold Memory PID output must exceed this to grant a slot
         * @param cpuPGain Proportional gain for CPU
         * @param cpuIGain Integral gain for CPU
         * @param cpuDGain Derivative gain for CPU
         * @param cpuOutputThreshold CPU PID output must exceed this to grant a slot
         */
        data class PidTuning(
            val memoryPGain: Double = 5.0,
            val memoryIGain: Double = 0.0,
            val memoryDGain: Double = 1.0,
            val memoryOutputThreshold: Double = 0.25,
            val cpuPGain: Double = 5.0,
            val cpuIGain: Double = 0.0,
            val cpuDGain: Double = 1.0,
            val cpuOutputThreshold: Double = 0.05,
        )
    }

    /**
     * Core SDK's built in slot supplier
     *
     * @param targetMemoryUsage Target system memory usage (0.0 to 1.0). Slots are denied when above this.
     * @param targetCpuUsage Target system CPU usage (0.0 to 1.0). Slots are denied when above this.
     * @param minimumSlots Minimum slots always granted regardless of resource pressure.
     * @param maximumSlots Hard upper bound on slot count.
     * @param rampThrottleMs Minimum delay between granting new slots, to prevent burst allocation.
     */
    @Deprecated(
        message = "CGroup monitoring is not recommended for JVM workers. Use JvmResourceBased instead.",
        replaceWith =
            ReplaceWith(
                "JvmResourceBased(targetMemoryUsage, targetCpuUsage, minimumSlots, maximumSlots, rampThrottleMs)",
            ),
    )
    data class CGroupResourceBased(
        val targetMemoryUsage: Double = 0.8,
        val targetCpuUsage: Double = 0.8,
        val minimumSlots: Int = 1,
        val maximumSlots: Int = 10000,
        val rampThrottleMs: Long = 50,
    ) : SlotSupplier()

    /**
     * Returns the maximum number of concurrent executions this supplier allows.
     */
    @Suppress("DEPRECATION")
    val maxConcurrent: Int
        get() =
            when (this) {
                is FixedSize -> slots
                is JvmResourceBased -> maximumSlots
                is CGroupResourceBased -> maximumSlots
            }
}
