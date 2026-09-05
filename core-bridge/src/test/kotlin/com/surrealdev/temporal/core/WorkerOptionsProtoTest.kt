package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.proto.WorkerOptions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class WorkerOptionsProtoTest {
    private fun encode(config: WorkerConfig = WorkerConfig()): WorkerOptions =
        WorkerOptions.parseFrom(WorkerOptionsProto.encode("queue", "namespace", config))

    @Test
    fun `every worker option reaches the generated schema including multi-byte tags`() {
        val wire =
            encode(
                WorkerConfig(
                    maxCachedWorkflows = 20,
                    workerIdentity = "identity",
                    enableActivities = false,
                    enableLocalActivities = false,
                    enableNexus = true,
                    buildId = "unversioned-build",
                    workflowSlotSupplier = SlotSupplier.FixedSize(7),
                    activitySlotSupplier = SlotSupplier.FixedSize(8),
                    localActivitySlotSupplier = SlotSupplier.FixedSize(9),
                    nexusSlotSupplier = SlotSupplier.FixedSize(11),
                    workflowPollerBehavior = CorePollerBehavior.SimpleMaximum(3),
                    activityPollerBehavior = CorePollerBehavior.Autoscaling(2, 8, 4),
                    nexusPollerBehavior = null,
                    maxHeartbeatThrottleIntervalMs = 500,
                    defaultHeartbeatThrottleIntervalMs = 250,
                    maxActivitiesPerSecond = 2.5,
                    maxTaskQueueActivitiesPerSecond = 3.5,
                    nonstickyToStickyPollRatio = 0.5f,
                    stickyQueueScheduleToStartTimeoutMs = 200,
                    gracefulShutdownPeriodMs = 100,
                    nondeterminismAsWorkflowFail = true,
                    nondeterminismAsWorkflowFailForTypes = listOf("Workflow"),
                    maxEagerActivityReservationsPerWorkflowTask = 0,
                    disablePayloadErrorLimit = true,
                ),
            )
        assertEquals("namespace", wire.namespace)
        assertEquals("queue", wire.taskQueue)
        assertEquals("identity", wire.identity)
        assertEquals(20, wire.maxCachedWorkflows)
        assertEquals("unversioned-build", wire.buildId)
        assertFalse(wire.noWorkflows)
        assertTrue(wire.noRemoteActivities && wire.noLocalActivities && wire.enableNexus)
        assertEquals(
            listOf(7, 8, 9, 11),
            listOf(
                wire.maxConcurrentWorkflowTasks,
                wire.maxConcurrentActivities,
                wire.maxConcurrentLocalActivities,
                wire.maxConcurrentNexusTasks,
            ),
        )
        assertEquals(3, wire.workflowPollerBehavior.simpleMaximum)
        assertEquals(2, wire.activityPollerBehavior.autoscaling.minimum)
        assertEquals(8, wire.activityPollerBehavior.autoscaling.maximum)
        assertEquals(4, wire.activityPollerBehavior.autoscaling.initial)
        assertFalse(wire.hasNexusPollerBehavior())
        assertEquals(500L, wire.maxHeartbeatThrottleIntervalMillis)
        assertEquals(250L, wire.defaultHeartbeatThrottleIntervalMillis)
        assertEquals(2.5, wire.maxActivitiesPerSecond)
        assertEquals(3.5, wire.maxTaskQueueActivitiesPerSecond)
        assertEquals(0.5f, wire.nonstickyToStickyPollRatio)
        assertEquals(200L, wire.stickyQueueScheduleToStartTimeoutMillis)
        assertEquals(100L, wire.gracefulShutdownPeriodMillis)
        assertTrue(wire.nondeterminismAsWorkflowFail)
        assertEquals(listOf("Workflow"), wire.nondeterminismAsWorkflowFailForTypesList)
        assertTrue(wire.hasMaxEagerActivityReservationsPerWorkflowTask())
        assertEquals(0, wire.maxEagerActivityReservationsPerWorkflowTask)
        assertTrue(wire.disablePayloadErrorLimit)
    }

    @Test
    fun `default values and explicit zero keep their presence`() {
        val defaults = encode()
        assertEquals(5, defaults.workflowPollerBehavior.simpleMaximum)
        assertEquals(5, defaults.activityPollerBehavior.simpleMaximum)
        assertEquals(2, defaults.nexusPollerBehavior.simpleMaximum)
        assertTrue(defaults.hasGracefulShutdownPeriodMillis())
        assertEquals(0L, defaults.gracefulShutdownPeriodMillis)
        assertEquals(3, defaults.maxEagerActivityReservationsPerWorkflowTask)
        val zero =
            encode(
                WorkerConfig(
                    maxCachedWorkflows = 0,
                    maxHeartbeatThrottleIntervalMs = 0,
                    gracefulShutdownPeriodMs = null,
                    workflowPollerBehavior = null,
                ),
            )
        assertEquals(0, zero.maxCachedWorkflows)
        assertTrue(zero.hasMaxHeartbeatThrottleIntervalMillis())
        assertEquals(0L, zero.maxHeartbeatThrottleIntervalMillis)
        assertFalse(zero.hasGracefulShutdownPeriodMillis())
        assertFalse(zero.hasWorkflowPollerBehavior())
    }

    @Test
    fun `deployment settings retain routing and workflow version behavior`() {
        val deployment =
            WorkerDeploymentOptions(WorkerDeploymentVersion("deployment", "build"), true, VersioningBehavior.PINNED)
        val wire = encode(WorkerConfig(deploymentOptions = deployment))
        assertEquals("deployment", wire.deploymentOptions.deploymentName)
        assertEquals("build", wire.deploymentOptions.buildId)
        assertTrue(wire.deploymentOptions.useWorkerVersioning)
        assertEquals(1, wire.deploymentOptions.defaultVersioningBehavior)
        assertFailsWith<IllegalArgumentException> {
            encode(WorkerConfig(deploymentOptions = deployment.copy(useWorkerVersioning = false)))
        }
    }

    @Test
    fun `JVM suppliers retain independent tuning and slot bounds for all task types`() {
        val wire =
            encode(
                WorkerConfig(
                    workflowSlotSupplier =
                        SlotSupplier.JvmResourceBased(
                            minimumSlots = 3,
                            maximumSlots = 42,
                            rampThrottleMs = 25,
                        ),
                    activitySlotSupplier =
                        SlotSupplier.JvmResourceBased(
                            targetMemoryUsage = 0.5,
                            maximumSlots = 10,
                        ),
                    nexusSlotSupplier =
                        SlotSupplier.JvmResourceBased(
                            minimumSlots = 0,
                            maximumSlots = 12,
                            rampThrottleMs = 0,
                        ),
                ),
            )
        assertTrue(wire.resourceTuner.jvmResourceBased)
        assertFalse(wire.hasMaxConcurrentWorkflowTasks())
        assertEquals(3, wire.workflowResourceLimits.minimumSlots)
        assertEquals(42, wire.workflowResourceLimits.maximumSlots)
        assertEquals(25L, wire.workflowResourceLimits.rampThrottleMillis)
        assertEquals(12, wire.nexusResourceLimits.maximumSlots)
        assertEquals(0L, wire.nexusResourceLimits.rampThrottleMillis)
        assertFalse(wire.hasLocalActivityResourceLimits())
        assertEquals(0.0, wire.resourceTuner.memoryIGain)
    }

    @Suppress("DEPRECATION")
    @Test
    fun `system suppliers reject conflicting targets and mixed resource sources`() {
        val config = WorkerConfig(workflowSlotSupplier = SlotSupplier.CGroupResourceBased())
        assertFalse(encode(config).resourceTuner.jvmResourceBased)
        assertFailsWith<IllegalArgumentException> {
            encode(config.copy(activitySlotSupplier = SlotSupplier.CGroupResourceBased(targetMemoryUsage = 0.5)))
        }
        assertFailsWith<IllegalArgumentException> {
            encode(config.copy(activitySlotSupplier = SlotSupplier.JvmResourceBased()))
        }
    }

    @Test
    fun `invalid options fail before protobuf can turn them into defaults`() {
        val invalid =
            listOf(
                WorkerConfig(maxCachedWorkflows = -1),
                WorkerConfig(activitySlotSupplier = SlotSupplier.FixedSize(0)),
                WorkerConfig(workflowSlotSupplier = SlotSupplier.FixedSize(1)),
                WorkerConfig(activitySlotSupplier = SlotSupplier.JvmResourceBased(minimumSlots = 5, maximumSlots = 4)),
                WorkerConfig(activitySlotSupplier = SlotSupplier.JvmResourceBased(maximumSlots = 0)),
                WorkerConfig(activitySlotSupplier = SlotSupplier.JvmResourceBased(rampThrottleMs = -1)),
                WorkerConfig(activitySlotSupplier = SlotSupplier.JvmResourceBased(targetMemoryUsage = Double.NaN)),
                WorkerConfig(
                    activitySlotSupplier =
                        SlotSupplier.JvmResourceBased(
                            pidTuning = SlotSupplier.JvmResourceBased.PidTuning(cpuPGain = Double.POSITIVE_INFINITY),
                        ),
                ),
                WorkerConfig(workflowPollerBehavior = CorePollerBehavior.SimpleMaximum(1)),
                WorkerConfig(activityPollerBehavior = CorePollerBehavior.Autoscaling(3, 2, 1)),
                WorkerConfig(maxActivitiesPerSecond = -1.0),
                WorkerConfig(maxTaskQueueActivitiesPerSecond = Double.NaN),
                WorkerConfig(nonstickyToStickyPollRatio = Float.POSITIVE_INFINITY),
                WorkerConfig(maxHeartbeatThrottleIntervalMs = -1),
                WorkerConfig(gracefulShutdownPeriodMs = -1),
                WorkerConfig(maxEagerActivityReservationsPerWorkflowTask = -1),
                WorkerConfig(enableWorkflows = false),
            )
        invalid.forEach { config -> assertFailsWith<IllegalArgumentException>(config.toString()) { encode(config) } }
    }
}
