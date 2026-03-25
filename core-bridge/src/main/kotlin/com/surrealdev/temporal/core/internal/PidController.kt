package com.surrealdev.temporal.core.internal

/**
 * Used by [CustomSlotSupplierBridge] to make adaptive slot grant decisions
 * based on JVM resource usage. A positive output indicates resources are
 * below the setpoint (safe to grant); negative means over the setpoint.
 **
 * @param setpoint Target value (e.g., 0.8 for 80% resource usage)
 * @param kp Proportional gain — reacts to current error magnitude
 * @param ki Integral gain — reacts to accumulated error over time
 * @param kd Derivative gain — reacts to rate of error change
 * @param outputLimit Clamp limit for output and integral windup prevention
 */
class PidController(
    private val setpoint: Double,
    private val kp: Double = 5.0,
    private val ki: Double = 0.0,
    private val kd: Double = 1.0,
    private val outputLimit: Double = DEFAULT_OUTPUT_LIMIT,
) {
    private var lastError: Double = 0.0
    private var integral: Double = 0.0

    /**
     * Computes the next control output given a new measurement.
     *
     * @param measurement Current resource usage (e.g., 0.72 for 72%)
     * @return Control output — positive means below setpoint (grant), negative means above (deny)
     */
    fun update(measurement: Double): Double {
        val error = setpoint - measurement
        integral = (integral + error).coerceIn(-outputLimit, outputLimit)
        val derivative = error - lastError
        lastError = error
        return (kp * error + ki * integral + kd * derivative)
            .coerceIn(-outputLimit, outputLimit)
    }

    companion object {
        const val DEFAULT_OUTPUT_LIMIT = 100.0
    }
}
