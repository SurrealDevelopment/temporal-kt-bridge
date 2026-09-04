package com.surrealdev.temporal.core.kt

import java.lang.foreign.Arena
import java.lang.foreign.FunctionDescriptor
import java.lang.foreign.Linker
import java.lang.foreign.MemorySegment
import java.lang.foreign.SymbolLookup
import java.lang.foreign.ValueLayout.ADDRESS
import java.lang.foreign.ValueLayout.JAVA_INT
import java.lang.foreign.ValueLayout.JAVA_LONG
import java.lang.invoke.MethodHandle
import java.nio.file.Path

/**
 * Hand-written FFM bindings to the kt-bridge ABI.
 *
 * The entire binding layer is this file. The C bridge needed 87 generated Java files and 21,589
 * lines to describe ~50 `#[repr(C)]` structs, and jextract was abandoned as too buggy to keep in
 * the loop, so those layouts were maintained by hand -- where a wrong offset compiled cleanly and
 * made Rust read past the end of a JVM-allocated struct. Here exactly one struct crosses the
 * boundary and it is all scalars, so it is read with fixed offsets and no `MemoryLayout` at all.
 */
internal object KtBridge {
    // KtCompletion: 48 bytes, naturally aligned, no padding. Verified against the native library
    // by [checkAbi] rather than trusted.
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
    const val KT_ERR_STALE_HANDLE = -3
    const val KT_ERR_WRONG_HANDLE_KIND = -4

    private val lookup: SymbolLookup
    private val linker = Linker.nativeLinker()

    init {
        val path =
            requireNotNull(System.getProperty("kt.bridge.libraryPath")) {
                "kt.bridge.libraryPath is not set; the Gradle test task supplies it"
            }
        System.load(path)
        lookup = SymbolLookup.libraryLookup(Path.of(path), Arena.global())
    }

    private fun handle(
        name: String,
        descriptor: FunctionDescriptor,
    ): MethodHandle =
        linker.downcallHandle(
            lookup.find(name).orElseThrow { UnsatisfiedLinkError("kt-bridge has no symbol $name") },
            descriptor,
        )

    private val abiProbe = handle("kt_abi_probe", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT))
    private val lastError = handle("kt_last_error", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT))
    private val runtimeNew =
        handle("kt_runtime_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS))
    private val runtimeFree = handle("kt_runtime_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))
    private val pollerNew =
        handle("kt_poller_new", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_INT, ADDRESS))
    private val pollerFree = handle("kt_poller_free", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))
    private val pollerWake = handle("kt_poller_wake", FunctionDescriptor.of(JAVA_INT, JAVA_LONG))

    // NOTE: deliberately NOT Linker.Option.critical(). This call blocks for as long as its
    // timeout, and `critical` would keep the thread in Java state, stalling every GC for the
    // duration. It must also only ever be called from a platform thread: a blocking downcall pins
    // and blocks its carrier, so a virtual thread would starve the scheduler.
    private val pollerPoll =
        handle(
            "kt_poller_poll",
            FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS),
        )

    init {
        checkAbi()
    }

    /**
     * Verifies the native library's ABI against the constants above.
     *
     * A mismatch means the library does not match this code; without this it surfaces much later
     * as corrupted memory rather than as a clear failure at load.
     */
    private fun checkAbi() {
        Arena.ofConfined().use { arena ->
            val cap = 32
            val buf = arena.allocate(JAVA_INT.byteSize() * cap)
            val n = abiProbe.invokeExact(buf, cap) as Int
            require(n in 1..cap) { "kt_abi_probe reported $n values" }
            val actual = IntArray(n) { buf.get(JAVA_INT, it * 4L) }
            val expected =
                intArrayOf(
                    0x4B544231, // magic "KTB1"
                    1, // abi version
                    RECORD_BYTES.toInt(),
                    O_REQ_ID.toInt(),
                    O_KIND.toInt(),
                    O_STATUS.toInt(),
                    O_PAYLOAD.toInt(),
                    O_PAYLOAD_LEN.toInt(),
                    O_AUX0.toInt(),
                    O_AUX1.toInt(),
                    64, // pointer width
                    11, // KtKind::COUNT
                )
            require(actual.contentEquals(expected)) {
                "kt-bridge ABI mismatch.\n  expected ${expected.toList()}\n  native   ${actual.toList()}"
            }
        }
    }

    fun lastError(): String =
        Arena.ofConfined().use { arena ->
            val cap = 4096
            val buf = arena.allocate(cap.toLong())
            val n = lastError.invokeExact(buf, cap) as Int
            if (n <=
                0
            ) {
                ""
            } else {
                String(buf.asSlice(0, minOf(n, cap).toLong()).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
            }
        }

    fun runtimeNew(config: ByteArray): Long =
        Arena.ofConfined().use { arena ->
            val cfg =
                if (config.isEmpty()) {
                    MemorySegment.NULL
                } else {
                    arena.allocateFrom(
                        java.lang.foreign.ValueLayout.JAVA_BYTE,
                        *config,
                    )
                }
            val out = arena.allocate(JAVA_LONG)
            check(runtimeNew.invokeExact(cfg, config.size, out) as Int == KT_OK) {
                "kt_runtime_new failed: ${lastError()}"
            }
            out.get(JAVA_LONG, 0)
        }

    fun runtimeFree(runtime: Long): Int = runtimeFree.invokeExact(runtime) as Int

    fun pollerNew(runtime: Long): Long =
        Arena.ofConfined().use { arena ->
            val out = arena.allocate(JAVA_LONG)
            check(pollerNew.invokeExact(runtime, 0, out) as Int == KT_OK) {
                "kt_poller_new failed: ${lastError()}"
            }
            out.get(JAVA_LONG, 0)
        }

    fun pollerFree(poller: Long): Int = pollerFree.invokeExact(poller) as Int

    fun pollerWake(poller: Long): Int = pollerWake.invokeExact(poller) as Int

    fun poll(
        poller: Long,
        batch: MemorySegment,
        cap: Int,
        timeoutMillis: Int,
        outCount: MemorySegment,
    ): Int = pollerPoll.invokeExact(poller, batch, cap, timeoutMillis, outCount) as Int
}
