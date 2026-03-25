package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.JvmResourceMonitor
import java.util.concurrent.Executors

/**
 * Creates a new [JvmResourceMonitor] scoped to the application lifecycle.
 *
 * The returned [AutoCloseable] must be closed when the application shuts down
 * to stop the background sampling thread.
 */
fun createJvmResourceMonitor(): AutoCloseable {
    val executor =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread.ofVirtual().name("jvm-resource-monitor").unstarted(runnable)
        }
    return JvmResourceMonitor(executor)
}
