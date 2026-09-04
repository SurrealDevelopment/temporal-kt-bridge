package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.internal.NativeLoader
import java.lang.foreign.Arena
import java.lang.foreign.FunctionDescriptor
import java.lang.foreign.Linker
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.ADDRESS
import java.lang.foreign.ValueLayout.JAVA_BYTE
import java.lang.foreign.ValueLayout.JAVA_INT
import java.lang.foreign.ValueLayout.JAVA_LONG
import java.lang.invoke.MethodHandle

/**
 * The entire FFM binding layer for kt-bridge.
 *
 * One file, because the ABI is one all-scalar struct plus scalars, `(ptr, len)` pairs and
 * protobuf config. The C API needed 87 generated Java files and 21,589 lines to mirror ~50
 * `#[repr(C)]` structs by hand, where a wrong offset compiled cleanly and made Rust read past the
 * end of a JVM-allocated struct. Here [checkAbi] verifies every offset against the native library
 * at class-init, so a mismatch is a startup error with a diff rather than corruption later.
 *
 * Nothing in this file registers an upcall. Results arrive by [Pump] draining a completion queue,
 * which is what removes the crash classes the C bridge kept working around: exceptions built on
 * Rust callback threads, stubs freed under in-flight callbacks, and JVM work on Tokio threads.
 */
internal object KtBridge {
    /** `KtCompletion` is 48 bytes of naturally-aligned scalars; verified by [checkAbi]. */
    const val RECORD_BYTES = 48L
    const val O_REQ_ID = 0L
    const val O_KIND = 8L
    const val O_STATUS = 12L
    const val O_PAYLOAD = 16L
    const val O_PAYLOAD_LEN = 24L
    const val O_AUX0 = 32L
    const val O_AUX1 = 40L

    const val KT_OK = 0
    const val KT_ERR_PANIC = -1
    const val KT_ERR_INVALID_ARGUMENT = -2
    const val KT_ERR_STALE_HANDLE = -3
    const val KT_ERR_WRONG_HANDLE_KIND = -4
    const val KT_ERR_SHUTDOWN = -5
    const val KT_ERR_WORKER_SHUT_DOWN = -6
    const val KT_ERR_CANCELLED = -7
    const val KT_ERR_FAILED = -8
    const val KT_ERR_BUFFER_TOO_SMALL = -9

    private val linker: Linker = Linker.nativeLinker()
    private val lookup = NativeLoader.loadKtBridge()

    private fun handle(
        name: String,
        descriptor: FunctionDescriptor,
    ): MethodHandle =
        linker.downcallHandle(
            lookup.find(name).orElseThrow { UnsatisfiedLinkError("kt-bridge has no symbol $name") },
            descriptor,
        )

    private val abiProbe = handle("kt_abi_probe", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT))
    private val lastErrorFn = handle("kt_last_error", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT))

    private val runtimeNewFn =
        handle("kt_runtime_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS))
    private val runtimeFreeFn = handle("kt_runtime_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))

    private val pollerNewFn =
        handle("kt_poller_new", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_INT, ADDRESS))
    private val pollerFreeFn = handle("kt_poller_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))
    private val pollerWakeFn = handle("kt_poller_wake", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))

    // NOTE: deliberately NOT Linker.Option.critical(). This blocks for up to its timeout, and
    // `critical` keeps the thread in Java state -- every GC would stall for that long. It must
    // also only be called from a platform thread: a blocking downcall pins its carrier, so a
    // virtual thread would starve the scheduler. [Pump] enforces both.
    private val pollerPollFn =
        handle(
            "kt_poller_poll",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS),
        )

    private val cancelFn = handle("kt_cancel", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG))

    private val clientConnectFn =
        handle(
            "kt_client_connect",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_INT, JAVA_LONG),
        )
    private val clientRpcFn =
        handle(
            "kt_client_rpc",
            FunctionDescriptor.of(
                JAVA_INT,
                JAVA_LONG,
                JAVA_LONG,
                JAVA_INT,
                ADDRESS,
                JAVA_INT,
                ADDRESS,
                JAVA_INT,
                JAVA_LONG,
            ),
        )
    private val clientFreeFn = handle("kt_client_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))

    private val workerNewFn =
        handle(
            "kt_worker_new",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_INT, ADDRESS),
        )
    private val workerStartFn =
        handle("kt_worker_start", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG))
    private val workerCompleteFn =
        handle(
            "kt_worker_complete",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG),
        )
    private val workerHeartbeatFn =
        handle("kt_worker_heartbeat", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_INT))
    private val workerShutdownFn =
        handle(
            "kt_worker_shutdown",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG),
        )
    private val workerFreeFn = handle("kt_worker_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))

    private val ephemeralStartFn =
        handle(
            "kt_ephemeral_start",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_INT, JAVA_LONG),
        )
    private val ephemeralInfoFn =
        handle(
            "kt_ephemeral_info",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_INT, ADDRESS),
        )
    private val ephemeralShutdownFn =
        handle(
            "kt_ephemeral_shutdown",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG),
        )
    private val ephemeralFreeFn =
        handle("kt_ephemeral_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))

    init {
        // Must run after every handle above is bound: Kotlin initialises in declaration order.
        checkAbi()
    }

    /**
     * Fails at class-init if the native library's layout is not what this file reads it with.
     *
     * Without this a mismatched library surfaces much later as corrupted memory, which is the
     * failure mode the C bridge's hand-maintained `$LAYOUT` declarations were prone to.
     */
    private fun checkAbi() {
        Arena.ofConfined().use { arena ->
            val cap = 32
            val buf = arena.allocate(JAVA_INT, cap.toLong())
            val n = abiProbe.invokeExact(buf, cap) as Int
            require(n in 1..cap) { "kt_abi_probe reported $n values" }
            val actual = IntArray(n) { buf.get(JAVA_INT, it * 4L) }
            val expected =
                intArrayOf(
                    0x4B544231,
                    1,
                    RECORD_BYTES.toInt(),
                    O_REQ_ID.toInt(),
                    O_KIND.toInt(),
                    O_STATUS.toInt(),
                    O_PAYLOAD.toInt(),
                    O_PAYLOAD_LEN.toInt(),
                    O_AUX0.toInt(),
                    O_AUX1.toInt(),
                    64,
                    11,
                )
            require(actual.contentEquals(expected)) {
                "kt-bridge ABI mismatch: the native library does not match this JAR.\n" +
                    "  expected ${expected.toList()}\n  native   ${actual.toList()}"
            }
        }
    }

    /** The calling thread's last error. Thread-local, so call it on the thread that failed. */
    fun lastError(): String =
        Arena.ofConfined().use { arena ->
            val cap = 8192
            val buf = arena.allocate(cap.toLong())
            val n = lastErrorFn.invokeExact(buf, cap) as Int
            if (n <= 0) "" else String(buf.asSlice(0, minOf(n, cap).toLong()).toArray(JAVA_BYTE), Charsets.UTF_8)
        }

    private fun Arena.bytes(value: ByteArray): MemorySegment =
        if (value.isEmpty()) MemorySegment.NULL else allocateFrom(JAVA_BYTE, *value)

    private fun check(
        code: Int,
        what: String,
    ) {
        if (code != KT_OK) throw KtBridgeException(code, "$what failed (${lastError().ifEmpty { "code $code" }})")
    }

    private fun outHandle(arena: Arena) = arena.allocate(JAVA_LONG)

    fun runtimeNew(config: ByteArray): Long =
        Arena.ofConfined().use { arena ->
            val out = outHandle(arena)
            check(runtimeNewFn.invokeExact(arena.bytes(config), config.size, out) as Int, "kt_runtime_new")
            out.get(JAVA_LONG, 0)
        }

    fun runtimeFree(runtime: Long): Int = runtimeFreeFn.invokeExact(runtime) as Int

    fun pollerNew(runtime: Long): Long =
        Arena.ofConfined().use { arena ->
            val out = outHandle(arena)
            check(pollerNewFn.invokeExact(runtime, 0, out) as Int, "kt_poller_new")
            out.get(JAVA_LONG, 0)
        }

    fun pollerFree(poller: Long): Int = pollerFreeFn.invokeExact(poller) as Int

    fun pollerWake(poller: Long): Int = pollerWakeFn.invokeExact(poller) as Int

    fun poll(
        poller: Long,
        batch: MemorySegment,
        cap: Int,
        timeoutMillis: Int,
        outCount: MemorySegment,
    ): Int = pollerPollFn.invokeExact(poller, batch, cap, timeoutMillis, outCount) as Int

    fun cancel(
        runtime: Long,
        reqId: Long,
    ): Int = cancelFn.invokeExact(runtime, reqId) as Int

    fun clientConnect(
        runtime: Long,
        config: ByteArray,
        reqId: Long,
    ) = Arena.ofConfined().use { arena ->
        check(
            clientConnectFn.invokeExact(runtime, arena.bytes(config), config.size, reqId) as Int,
            "kt_client_connect",
        )
    }

    fun clientRpc(
        runtime: Long,
        client: Long,
        service: Int,
        rpc: String,
        request: ByteArray,
        reqId: Long,
    ) = Arena.ofConfined().use { arena ->
        val name = rpc.toByteArray(Charsets.UTF_8)
        check(
            clientRpcFn.invokeExact(
                runtime,
                client,
                service,
                arena.bytes(name),
                name.size,
                arena.bytes(request),
                request.size,
                reqId,
            ) as Int,
            "kt_client_rpc",
        )
    }

    fun clientFree(client: Long): Int = clientFreeFn.invokeExact(client) as Int

    fun workerNew(
        runtime: Long,
        client: Long,
        config: ByteArray,
    ): Long =
        Arena.ofConfined().use { arena ->
            val out = outHandle(arena)
            check(
                workerNewFn.invokeExact(runtime, client, arena.bytes(config), config.size, out) as Int,
                "kt_worker_new",
            )
            out.get(JAVA_LONG, 0)
        }

    fun workerStart(
        runtime: Long,
        worker: Long,
    ) = check(workerStartFn.invokeExact(runtime, worker) as Int, "kt_worker_start")

    fun workerComplete(
        runtime: Long,
        worker: Long,
        taskKind: Int,
        proto: ByteArray,
        reqId: Long,
    ) = Arena.ofConfined().use { arena ->
        check(
            workerCompleteFn.invokeExact(runtime, worker, taskKind, arena.bytes(proto), proto.size, reqId) as Int,
            "kt_worker_complete",
        )
    }

    /** Returns the status rather than throwing: a heartbeat racing shutdown is expected. */
    fun workerHeartbeat(
        worker: Long,
        proto: ByteArray,
    ): Int =
        Arena.ofConfined().use { arena ->
            workerHeartbeatFn.invokeExact(worker, arena.bytes(proto), proto.size) as Int
        }

    fun workerShutdown(
        runtime: Long,
        worker: Long,
        graceMillis: Long,
        reqId: Long,
    ) = check(workerShutdownFn.invokeExact(runtime, worker, graceMillis, reqId) as Int, "kt_worker_shutdown")

    fun workerFree(worker: Long): Int = workerFreeFn.invokeExact(worker) as Int

    fun ephemeralStart(
        runtime: Long,
        config: ByteArray,
        reqId: Long,
    ) = Arena.ofConfined().use { arena ->
        check(
            ephemeralStartFn.invokeExact(runtime, arena.bytes(config), config.size, reqId) as Int,
            "kt_ephemeral_start",
        )
    }

    fun ephemeralInfo(server: Long): ByteArray =
        Arena.ofConfined().use { arena ->
            val cap = 4096
            val buf = arena.allocate(cap.toLong())
            val outLen = arena.allocate(JAVA_INT)
            check(ephemeralInfoFn.invokeExact(server, buf, cap, outLen) as Int, "kt_ephemeral_info")
            buf.asSlice(0, outLen.get(JAVA_INT, 0).toLong()).toArray(JAVA_BYTE)
        }

    fun ephemeralShutdown(
        runtime: Long,
        server: Long,
        reqId: Long,
    ) = check(ephemeralShutdownFn.invokeExact(runtime, server, reqId) as Int, "kt_ephemeral_shutdown")

    fun ephemeralFree(server: Long): Int = ephemeralFreeFn.invokeExact(server) as Int
}

/** A failure reported by the native bridge, carrying its status code. */
internal class KtBridgeException(
    val code: Int,
    message: String,
) : RuntimeException(message)
