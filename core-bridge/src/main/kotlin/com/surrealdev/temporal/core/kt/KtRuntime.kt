package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException
import java.util.concurrent.ConcurrentHashMap

/**
 * A Core runtime on the kt-bridge ABI.
 *
 * Owns the native runtime and the single [Pump] that drains its completion queue. Closing is
 * ordered so nothing is left waiting: freeing the native runtime answers every outstanding
 * request with `KT_ERR_SHUTDOWN` and closes the queue, which releases the pump; the pump then
 * fails anything still registered. The C bridge instead blocked on a 60-second latch here,
 * because it had no guarantee a callback would ever arrive.
 *
 * There is no metrics-bridge parameter. Core's own Prometheus and OTLP exporters replace the
 * C bridge's eight custom-meter upcalls, which ran on Tokio threads and had to be wrapped in
 * blanket `catch (Throwable)` to keep an exception from crossing the FFI boundary.
 */
internal class KtRuntime private constructor(
    val handle: Long,
) : AutoCloseable {
    /** Consumers of pushed (unsolicited) completions, keyed by the worker handle in `aux0`. */
    private val pushedHandlers = ConcurrentHashMap<Long, (Completion) -> Unit>()

    val pump: Pump = Pump(handle, ::onPushed)

    @Volatile
    private var closed = false

    private fun onPushed(completion: Completion) {
        when (completion.kind) {
            Kind.SERVER_LOG, Kind.LOG -> Unit // wired up with log forwarding
            else -> pushedHandlers[completion.aux0]?.invoke(completion)
        }
    }

    /** Routes this worker's pushed tasks to [handler]. */
    fun onWorkerEvents(
        worker: Long,
        handler: (Completion) -> Unit,
    ) {
        pushedHandlers[worker] = handler
    }

    fun removeWorkerEvents(worker: Long) {
        pushedHandlers.remove(worker)
    }

    fun ensureOpen() {
        check(!closed) { "Runtime has been closed" }
    }

    override fun close() {
        if (closed) return
        synchronized(this) {
            if (closed) return
            closed = true
            // Free first: this answers outstanding requests and closes the queue, which is what
            // lets the pump's blocking poll return instead of parking forever.
            KtBridge.runtimeFree(handle)
            pump.close()
            pushedHandlers.clear()
        }
    }

    companion object {
        /**
         * @param config an encoded `kt_bridge.RuntimeOptions`, or empty for defaults.
         */
        fun create(config: ByteArray = ByteArray(0)): KtRuntime =
            try {
                KtRuntime(KtBridge.runtimeNew(config))
            } catch (e: KtBridgeException) {
                throw TemporalCoreException(
                    message = "could not create the Temporal runtime: ${e.message}",
                    errorType = null,
                    statusCode = e.code,
                    cause = e,
                    // A real Java stack: this runs on the caller's thread, not a Rust callback
                    // thread, where building one used to crash the JVM in fillInStackTrace.
                    writableStackTrace = true,
                )
            }
    }
}
