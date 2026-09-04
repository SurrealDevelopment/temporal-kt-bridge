package com.surrealdev.temporal.core

import coresdk.activity_task.ActivityTaskOuterClass
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.time.Duration.Companion.milliseconds

/**
 * The encoder test proves the bytes are right; this proves Core accepts them.
 *
 * `build_tuner_holder` validates its options and fails the worker construction, so a worker that
 * starts and polls is the evidence that the resource-based supplier was actually built.
 */
class ResourceTunerWorkerTest {
    @Test
    fun `a resource-tuned worker starts, polls and shuts down`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                TemporalDevServer.start(runtime).use { server ->
                    TemporalCoreClient.connect(runtime, server.targetUrl, "default").use { client ->
                        val worker =
                            TemporalWorker.create(
                                runtime,
                                client,
                                "resource-tuned-queue",
                                "default",
                                WorkerConfig(
                                    workflowSlotSupplier = SlotSupplier.JvmResourceBased(maximumSlots = 20),
                                    activitySlotSupplier = SlotSupplier.JvmResourceBased(maximumSlots = 30),
                                    // Deliberately mixed: Core must accept one holder carrying
                                    // both supplier kinds.
                                    localActivitySlotSupplier = SlotSupplier.FixedSize(5),
                                ),
                            )
                        val poller =
                            launch {
                                worker.pollActivityTask { ActivityTaskOuterClass.ActivityTask.parseFrom(it) }
                            }
                        delay(300.milliseconds)
                        worker.initiateShutdown()
                        poller.join()
                        worker.awaitShutdown()
                        worker.close()
                    }
                }
            }
        }
}
