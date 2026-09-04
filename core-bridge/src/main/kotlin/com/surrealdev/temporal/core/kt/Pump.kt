package com.surrealdev.temporal.core.kt

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendCancellableCoroutine
import org.slf4j.LoggerFactory
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.JAVA_INT
import java.lang.foreign.ValueLayout.JAVA_LONG
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/** One completion, copied out of the native batch buffer before the next poll invalidates it. */
internal class Completion(
    val reqId: Long,
    val kind: Int,
    val status: Int,
    val payload: ByteArray,
    val aux0: Long,
    val aux1: Long,
) {
    val isFailure: Boolean get() = status != KtBridge.KT_OK

    /** The payload is the error message when the status is non-zero. */
    fun errorMessage(): String = String(payload, Charsets.UTF_8)
}

/** Completion kinds; must match `KtKind` in abi.rs, which `kt_abi_probe` pins the count of. */
internal object Kind {
    const val ACK = 0
    const val CLIENT_CONNECTED = 1
    const val RPC = 2
    const val TASK_WORKFLOW_ACTIVATION = 3
    const val TASK_ACTIVITY = 4
    const val TASK_NEXUS = 5
    const val TASK_STREAM_END = 6
    const val WORKER_FAILED = 7
    const val EPHEMERAL_STARTED = 8
    const val SERVER_LOG = 9
    const val LOG = 10
}

/**
 * Drains the native completion queue on a dedicated thread and resumes waiting coroutines.
 *
 * This is what replaces FFM upcalls. Under the C bridge, Rust called into the JVM on Tokio
 * threads, where constructing any exception could crash the VM in `fillInStackTrace`, an arena
 * freed by coroutine cancellation could leave a stub dangling, and every callback had to hop off
 * the thread before doing real work. Here Rust never calls the JVM at all: this thread asks for
 * results and does the work itself, on an ordinary Java stack where exceptions, logging and
 * blocking are all simply fine.
 *
 * Cancellation is real again, too. Because Rust guarantees exactly one terminal completion per
 * request -- including for a cancelled one -- a cancelled coroutine can just tell Rust and walk
 * away, instead of the C bridge's rule that no native call could be cancelled at all.
 */
internal class Pump(
    private val runtime: Long,
    private val onPushed: (Completion) -> Unit,
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(Pump::class.java)
    private val poller = KtBridge.pollerNew(runtime)
    private val pending = ConcurrentHashMap<Long, CancellableContinuation<Completion>>()
    private val nextReqId = AtomicLong(1)

    @Volatile
    private var running = true

    // A platform thread, never virtual: kt_poller_poll blocks, and a blocking downcall pins its
    // carrier, which would starve the scheduler.
    private val thread =
        Thread
            .ofPlatform()
            .daemon()
            .name("temporal-pump-$runtime")
            .unstarted(::run)

    init {
        thread.start()
    }

    /** Allocates a request id. Ids stay below 2^48; Rust allocates its own above that. */
    fun nextRequestId(): Long = nextReqId.getAndIncrement()

    /**
     * Issues a request and suspends until its single terminal completion arrives.
     *
     * [start] must actually start the operation; if it throws, the request is retired here so no
     * id is left registered.
     */
    suspend fun request(start: (Long) -> Unit): Completion {
        val reqId = nextRequestId()
        return suspendCancellableCoroutine { continuation ->
            pending[reqId] = continuation
            continuation.invokeOnCancellation {
                // Rust still answers exactly once; the completion is simply discarded because the
                // continuation is already gone.
                KtBridge.cancel(runtime, reqId)
            }
            try {
                start(reqId)
            } catch (t: Throwable) {
                pending.remove(reqId)
                if (continuation.isActive) continuation.resumeWithException(t)
            }
        }
    }

    private fun run() {
        Arena.ofShared().use { arena ->
            val batch = arena.allocate(KtBridge.RECORD_BYTES * BATCH, 8)
            val count = arena.allocate(JAVA_INT)
            while (running) {
                val rc = KtBridge.poll(poller, batch, BATCH, POLL_TIMEOUT_MILLIS, count)
                if (rc != KtBridge.KT_OK) {
                    // The queue is closed, or the poller is gone: either way there is nothing
                    // more to drain.
                    if (running) logger.debug("pump stopping, poll returned {}", rc)
                    break
                }
                repeat(count.get(JAVA_INT, 0)) { dispatch(batch, it * KtBridge.RECORD_BYTES) }
            }
        }
        failPending()
    }

    private fun dispatch(
        batch: MemorySegment,
        offset: Long,
    ) {
        val length = batch.get(JAVA_LONG, offset + KtBridge.O_PAYLOAD_LEN)
        val address = batch.get(JAVA_LONG, offset + KtBridge.O_PAYLOAD)
        // Copied now: the payload lives in the poller's slab and the next poll reuses it.
        val payload =
            if (length == 0L) {
                EMPTY
            } else {
                MemorySegment.ofAddress(address).reinterpret(length).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE)
            }

        val completion =
            Completion(
                reqId = batch.get(JAVA_LONG, offset + KtBridge.O_REQ_ID),
                kind = batch.get(JAVA_INT, offset + KtBridge.O_KIND),
                status = batch.get(JAVA_INT, offset + KtBridge.O_STATUS),
                payload = payload,
                aux0 = batch.get(JAVA_LONG, offset + KtBridge.O_AUX0),
                aux1 = batch.get(JAVA_LONG, offset + KtBridge.O_AUX1),
            )

        if (completion.reqId == 0L) {
            // Pushed: a task, a log line, a server event. Never blocks the pump for long -- the
            // consumer is expected to hand off rather than process inline.
            runCatching { onPushed(completion) }
                .onFailure { logger.error("pushed completion handler threw", it) }
            return
        }
        // A cancelled request is already gone from the map; its completion is simply dropped.
        pending.remove(completion.reqId)?.let { continuation ->
            if (continuation.isActive) continuation.resume(completion)
        }
    }

    /**
     * Fails everything still waiting once the pump stops.
     *
     * Rust already answers outstanding requests on shutdown, so this is a backstop for the case
     * where the pump itself stops first. Either way no coroutine is left suspended forever, which
     * is what let the C bridge's 60-second shutdown latch go away.
     */
    private fun failPending() {
        val stragglers = pending.keys.toList()
        stragglers.forEach { reqId ->
            pending.remove(reqId)?.let { continuation ->
                if (continuation.isActive) {
                    continuation.resumeWithException(
                        KtBridgeException(KtBridge.KT_ERR_SHUTDOWN, "bridge shut down before request $reqId completed"),
                    )
                }
            }
        }
    }

    override fun close() {
        running = false
        KtBridge.pollerWake(poller)
        thread.join(CLOSE_TIMEOUT_MILLIS)
        KtBridge.pollerFree(poller)
        failPending()
    }

    private companion object {
        const val BATCH = 64

        /** Bounded so `running` is observed even when the queue is idle. */
        const val POLL_TIMEOUT_MILLIS = 250
        const val CLOSE_TIMEOUT_MILLIS = 5_000L
        val EMPTY = ByteArray(0)
    }
}
