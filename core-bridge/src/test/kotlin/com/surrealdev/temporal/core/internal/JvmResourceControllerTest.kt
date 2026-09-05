package com.surrealdev.temporal.core.internal

import com.surrealdev.temporal.core.SlotSupplier
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JvmResourceControllerTest {
    @Test
    fun `JVM pressure and per-type tuning control grants`() {
        val options = SlotSupplier.JvmResourceBased(rampThrottleMs = 0)
        val normal = JvmResourceController(options)
        val conservative =
            JvmResourceController(options.copy(pidTuning = options.pidTuning.copy(memoryOutputThreshold = 10.0)))
        assertTrue(normal.update(0.4, 0.2))
        assertFalse(conservative.update(0.4, 0.2))
        assertFalse(normal.update(0.9, 0.2))
        assertFalse(normal.update(0.2, 0.95))
        assertTrue(normal.update(0.2, 0.2))
    }

    @Test
    fun `ramp throttle preserves PID history until next decision`() {
        val controller = JvmResourceController(SlotSupplier.JvmResourceBased(rampThrottleMs = 100))
        assertTrue(controller.update(0.2, 0.2, 0))
        assertFalse(controller.update(0.9, 0.2, 99_000_000))
        assertFalse(controller.update(0.9, 0.2, 100_000_000))
    }
}
