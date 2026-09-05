package com.surrealdev.temporal.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The encoder is hand-rolled -- the bridge's own config protos are not published -- so these
 * check the wire bytes directly.
 *
 * The case that matters is a resource-based supplier: it used to fall through to fixed slots,
 * which is the worst kind of bug, because the worker still starts and merely ignores what the
 * caller asked for.
 */
class WorkerOptionsProtoTest {
    private fun fields(bytes: ByteArray): Map<Int, MutableList<ByteArray>> {
        val out = mutableMapOf<Int, MutableList<ByteArray>>()
        var i = 0

        fun varint(): Long {
            var result = 0L
            var shift = 0
            while (true) {
                val b = bytes[i++].toInt() and 0xFF
                result = result or ((b and 0x7F).toLong() shl shift)
                if (b < 0x80) return result
                shift += 7
            }
        }
        while (i < bytes.size) {
            val tag = varint()
            val number = (tag ushr 3).toInt()
            val payload =
                when ((tag and 7L).toInt()) {
                    0 -> {
                        val start = i
                        varint()
                        bytes.copyOfRange(start, i)
                    }
                    1 -> bytes.copyOfRange(i, i + 8).also { i += 8 }
                    2 -> {
                        val len = varint().toInt()
                        bytes.copyOfRange(i, i + len).also { i += len }
                    }
                    else -> error("unexpected wire type in field $number")
                }
            out.getOrPut(number) { mutableListOf() }.add(payload)
        }
        return out
    }

    private fun double(b: ByteArray): Double {
        var bits = 0L
        for (k in 7 downTo 0) bits = (bits shl 8) or (b[k].toLong() and 0xFF)
        return java.lang.Double.longBitsToDouble(bits)
    }

    @Test
    fun `fixed suppliers encode as slot counts and no tuner`() {
        val encoded =
            WorkerOptionsProto.encode(
                taskQueue = "q",
                namespace = "ns",
                config =
                    WorkerConfig(
                        workflowSlotSupplier = SlotSupplier.FixedSize(7),
                        activitySlotSupplier = SlotSupplier.FixedSize(8),
                        localActivitySlotSupplier = SlotSupplier.FixedSize(9),
                    ),
            )
        val f = fields(encoded)
        assertEquals(7, f.getValue(5).single()[0].toInt())
        assertEquals(8, f.getValue(6).single()[0].toInt())
        assertEquals(9, f.getValue(7).single()[0].toInt())
        assertTrue(10 !in f, "no resource tuner should be sent for fixed suppliers")
        assertTrue(11 !in f && 12 !in f && 13 !in f, "no per-slot limits for fixed suppliers")
    }

    @Test
    fun `a resource-based supplier sends targets, gains and its own limits`() {
        val encoded =
            WorkerOptionsProto.encode(
                taskQueue = "q",
                namespace = "ns",
                config =
                    WorkerConfig(
                        workflowSlotSupplier =
                            SlotSupplier.JvmResourceBased(
                                targetMemoryUsage = 0.7,
                                targetCpuUsage = 0.6,
                                minimumSlots = 3,
                                maximumSlots = 42,
                                rampThrottleMs = 25,
                            ),
                        activitySlotSupplier = SlotSupplier.FixedSize(8),
                        localActivitySlotSupplier = SlotSupplier.FixedSize(9),
                    ),
            )
        val f = fields(encoded)

        val tuner = fields(f.getValue(10).single())
        assertEquals(0.7, double(tuner.getValue(1).single()))
        assertEquals(0.6, double(tuner.getValue(2).single()))
        // Core's defaults, which the JVM's PidTuning mirrors exactly.
        assertEquals(5.0, double(tuner.getValue(3).single()))
        assertEquals(0.25, double(tuner.getValue(6).single()))
        assertEquals(0.05, double(tuner.getValue(10).single()))

        val limits = fields(f.getValue(11).single())
        assertEquals(3, limits.getValue(1).single()[0].toInt())
        assertEquals(42, limits.getValue(2).single()[0].toInt())
        assertEquals(25, limits.getValue(3).single()[0].toInt())

        // The other two stay fixed: a worker may mix the two kinds.
        assertTrue(12 !in f && 13 !in f, "fixed slot types must not send resource limits")
        assertEquals(8, f.getValue(6).single()[0].toInt())
    }

    @Test
    fun `an i_gain of zero survives the wire`() {
        val encoded =
            WorkerOptionsProto.encode(
                "q",
                "ns",
                WorkerConfig(workflowSlotSupplier = SlotSupplier.JvmResourceBased()),
            )
        val tuner = fields(fields(encoded).getValue(10).single())
        // The whole reason the gains are fixed-width: a varint encoder would have dropped these
        // as "unset", and Core would have silently substituted its own default.
        assertEquals(0.0, double(tuner.getValue(4).single()))
        assertEquals(0.0, double(tuner.getValue(8).single()))
    }

    @Test
    fun `worker identity and task-type toggles reach the wire`() {
        val encoded =
            WorkerOptionsProto.encode(
                "q",
                "ns",
                WorkerConfig(
                    workerIdentity = "payment-worker",
                    enableActivities = false,
                    enableLocalActivities = false,
                ),
            )
        val f = fields(encoded)
        assertEquals("payment-worker", String(f.getValue(3).single()))
        // Negated on the wire, so "disabled" is what shows up.
        assertEquals(1, f.getValue(8).single()[0].toInt(), "enableActivities=false -> no_remote_activities")
        assertEquals(1, f.getValue(15).single()[0].toInt(), "enableLocalActivities=false -> no_local_activities")
        assertTrue(14 !in f, "workflows stay enabled, so nothing is sent")
    }

    @Test
    fun `no identity means the bridge derives one`() {
        val f = fields(WorkerOptionsProto.encode("q", "ns", WorkerConfig()))
        assertTrue(3 !in f, "an unset identity must not be sent as an empty string")
    }
}
