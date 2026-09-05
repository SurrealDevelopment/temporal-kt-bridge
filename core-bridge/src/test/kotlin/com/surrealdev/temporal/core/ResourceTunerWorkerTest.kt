package com.surrealdev.temporal.core

import coresdk.activity_task.ActivityTaskOuterClass
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
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

    @Test
    fun `closing a worker from its metrics callback preserves sampling`(): Unit =
        runBlocking {
            val samplerThread = CompletableDeferred<Thread>()
            TemporalRuntime.create().use { runtime ->
                TemporalDevServer.start(runtime).use { server ->
                    TemporalCoreClient.connect(runtime, server.targetUrl, "default").use { client ->
                        val config =
                            WorkerConfig(
                                workflowSlotSupplier = SlotSupplier.JvmResourceBased(maximumSlots = 20),
                                activitySlotSupplier = SlotSupplier.FixedSize(5),
                                localActivitySlotSupplier = SlotSupplier.FixedSize(5),
                            )
                        val firstWorker =
                            TemporalWorker.create(
                                runtime,
                                client,
                                "resource-callback-close",
                                "default",
                                config,
                            )
                        firstWorker.use { first ->
                            val secondWorker =
                                TemporalWorker.create(
                                    runtime,
                                    client,
                                    "resource-survivor",
                                    "default",
                                    config,
                                )
                            secondWorker.use { second ->
                                val closed = CompletableDeferred<Unit>()
                                val sampledAgain = CompletableDeferred<Unit>()
                                var subsequentSamples = 0
                                first.onSlotSupplierMetrics {
                                    samplerThread.complete(Thread.currentThread())
                                    first.close()
                                    closed.complete(Unit)
                                }
                                second.onSlotSupplierMetrics {
                                    // One workflow sample per tick: require a later tick to prove
                                    // callback removal did not kill the scheduled sampler.
                                    if (closed.isCompleted && ++subsequentSamples >= 2) sampledAgain.complete(Unit)
                                }
                                withTimeout(5_000) {
                                    closed.await()
                                    sampledAgain.await()
                                }
                                assertTrue(first.isClosed())
                                assertFalse(second.isClosed())
                                second.initiateShutdown()
                                second.awaitShutdown()
                            }
                        }
                    }
                }
            }
            val thread = samplerThread.await()
            thread.join(2_000)
            assertFalse(thread.isAlive, "closing the runtime must terminate its sampler thread")
        }
}
