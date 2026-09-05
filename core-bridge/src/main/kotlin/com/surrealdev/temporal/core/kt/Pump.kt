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

    /** Set once the pump thread has exited; a request issued after this can never be answered. */

    @Volatile
    private var dead = false

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
            if (dead || !running) {
                // Ordered after the insert on purpose: failPending() runs after `dead` is set, so
                // a request that slips in between is either seen by it or fails here. Never
                // neither.
                if (pending.remove(reqId) != null) {
                    continuation.resumeWithException(
                        KtBridgeException(
                            KtBridge.KT_ERR_SHUTDOWN,
                            "bridge pump has stopped; request $reqId not issued",
                        ),
                    )
                }
                return@suspendCancellableCoroutine
            }
            try {
                start(reqId)
            } catch (t: Throwable) {
                pending.remove(reqId)
                // A bridge-level failure to even issue the request surfaces in the public form.
                val mapped = if (t is KtBridgeException) t.asCore("request could not be issued") else t
                if (continuation.isActive) continuation.resumeWithException(mapped)
                return@suspendCancellableCoroutine
            }
            // Registered AFTER start(): Rust only knows the id once start() has issued it, so a
            // cancel that landed before then was a no-op and the operation would run to completion
            // with nobody listening -- leaking whatever it created. If the coroutine is already
            // cancelled by now, the handler runs immediately and kt_cancel finds the id.
            continuation.invokeOnCancellation {
                // Rust still answers exactly once; the completion is simply discarded because the
                // continuation is already gone.
                KtBridge.cancel(runtime, reqId)
            }
        }
    }

    private fun run() {
        try {
            Arena.ofShared().use { arena ->
                val batch = arena.allocate(KtBridge.RECORD_BYTES * BATCH, 8)
                val count = arena.allocate(JAVA_INT)
                while (true) {
                    // Once closing, drain queued resource results before releasing the poller.
                    val timeout = if (running) POLL_TIMEOUT_MILLIS else 0
                    val rc = KtBridge.poll(poller, batch, BATCH, timeout, count)
                    if (rc != KtBridge.KT_OK) {
                        // The queue is closed, or the poller is gone: either way there is nothing
                        // more to drain.
                        if (running) logger.debug("pump stopping, poll returned {}", rc)
                        break
                    }
                    val size = count.get(JAVA_INT, 0)
                    if (!running && size == 0) break
                    repeat(size) { dispatch(batch, it * KtBridge.RECORD_BYTES) }
                }
            }
        } catch (t: Throwable) {
            logger.error("pump thread died; failing every outstanding request", t)
        } finally {
            // In `finally`, not after the loop: if dispatch throws, the thread dies, and without
            // this every suspended caller -- and every future one -- would wait forever.
            dead = true
            failPending()
            // Only this thread may release the slab: close can time out or run inside a
            // resumed Unconfined coroutine while the rest of this batch is still being read.
            KtBridge.pollerFree(poller)
        }
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
        dispatch(completion)
    }

    internal fun dispatch(completion: Completion) {
        if (completion.reqId == 0L) {
            // Pushed: a task, a log line, a server event. Never blocks the pump for long -- the
            // consumer is expected to hand off rather than process inline.
            runCatching { onPushed(completion) }
                .onFailure { logger.error("pushed completion handler threw", it) }
            return
        }
        // A cancelled request is already gone from the map; its completion is dropped -- but not
        // what it may have created. A client or server that finished connecting/starting after
        // its caller gave up would otherwise sit in the native handle table forever, unknown to
        // anyone, with a child process outliving the JVM.
        val continuation = pending.remove(completion.reqId)
        if (continuation != null) {
            continuation.resume(completion) { _, discarded, _ -> releaseDiscarded(discarded) }
        } else {
            releaseDiscarded(completion)
        }
    }

    private fun releaseDiscarded(completion: Completion) {
        if (completion.isFailure) return
        when (completion.kind) {
            Kind.EPHEMERAL_STARTED -> KtBridge.ephemeralFree(completion.aux0) // Drop shuts the server down
            Kind.CLIENT_CONNECTED -> KtBridge.clientFree(completion.aux0)
            else -> Unit
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
        if (Thread.currentThread() !== thread) thread.join(CLOSE_TIMEOUT_MILLIS)
    }

    private companion object {
        const val BATCH = 64

        /** Bounded so `running` is observed even when the queue is idle. */
        const val POLL_TIMEOUT_MILLIS = 250
        const val CLOSE_TIMEOUT_MILLIS = 5_000L
        val EMPTY = ByteArray(0)
    }
}
