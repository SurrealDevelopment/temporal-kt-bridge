package com.surrealdev.temporal.core.kt

import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout.JAVA_INT
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Exercises the new bridge across the JVM boundary.
 *
 * The design's central claims -- no upcalls, one all-scalar struct, handles rather than pointers,
 * contained panics -- are only meaningful if they hold through FFM, not just in Rust unit tests.
 * Loading the library at all runs the ABI self-check in [KtBridge]'s initialiser.
 */
class KtBridgeAbiTest {
    @Test
    fun `the native library's ABI matches the constants this code reads it with`() {
        // Touch a real member: `const val`s are inlined at compile time, so reading one would
        // neither load the class nor run the ABI check. Reaching here at all means kt_abi_probe
        // agreed with every offset constant.
        KtBridge.lastError()
    }

    @Test
    fun `a runtime can be created and freed`() {
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        assertTrue(runtime != 0L, "handle 0 is never valid")
        assertEquals(KtBridge.KT_OK, KtBridge.runtimeFree(runtime))
    }

    @Test
    fun `a freed handle is reported stale rather than dereferenced`() {
        // The property that makes use-after-free an error code instead of undefined behaviour.
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        KtBridge.runtimeFree(runtime)
        assertEquals(KtBridge.KT_ERR_STALE_HANDLE, KtBridge.runtimeFree(runtime))
    }

    @Test
    fun `handle zero is rejected`() {
        assertEquals(KtBridge.KT_ERR_STALE_HANDLE, KtBridge.runtimeFree(0L))
    }

    @Test
    fun `passing a runtime handle where a poller is expected is a typed error`() {
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        try {
            assertEquals(KtBridge.KT_ERR_WRONG_HANDLE_KIND, KtBridge.pollerWake(runtime))
        } finally {
            KtBridge.runtimeFree(runtime)
        }
    }

    @Test
    fun `an empty poll returns no records without blocking`() {
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        val poller = KtBridge.pollerNew(runtime)
        try {
            Arena.ofConfined().use { arena ->
                val batch = arena.allocate(KtBridge.RECORD_BYTES * 8, 8)
                val count = arena.allocate(JAVA_INT)
                assertEquals(KtBridge.KT_OK, KtBridge.poll(poller, batch, 8, 0, count))
                assertEquals(0, count.get(JAVA_INT, 0))
            }
        } finally {
            KtBridge.pollerFree(poller)
            KtBridge.runtimeFree(runtime)
        }
    }

    @Test
    fun `freeing the runtime answers outstanding work and wakes a blocked poller`() {
        // This is the invariant that lets the JVM drop the C bridge's 60-second
        // "await pending callbacks" latch: shutdown must never leave a pump parked forever.
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        val poller = KtBridge.pollerNew(runtime)

        val blocked =
            Thread.ofPlatform().start {
                Arena.ofConfined().use { arena ->
                    val batch = arena.allocate(KtBridge.RECORD_BYTES * 4, 8)
                    val count = arena.allocate(JAVA_INT)
                    // -1 blocks indefinitely; only a wake can release it.
                    KtBridge.poll(poller, batch, 4, -1, count)
                }
            }

        Thread.sleep(200)
        assertTrue(blocked.isAlive, "the poll should still be parked before shutdown")

        KtBridge.runtimeFree(runtime)
        blocked.join(5_000)
        assertTrue(!blocked.isAlive, "runtime shutdown must release a blocked poller")

        KtBridge.pollerFree(poller)
    }

    @Test
    fun `a poller reports which runtime it belongs to via a valid handle`() {
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        val poller = KtBridge.pollerNew(runtime)
        assertTrue(poller != 0L)
        assertTrue(poller != runtime, "distinct objects must get distinct handles")
        KtBridge.pollerFree(poller)
        KtBridge.runtimeFree(runtime)
    }

    @Test
    fun `freeing a poller handle as a runtime is rejected and leaves both alive`() {
        // Regression: `remove` checked only index and generation, so kt_poller_free(runtimeHandle)
        // destroyed the runtime, returned KT_OK and skipped shutdown -- leaving every pump parked
        // forever on a runtime that no longer existed.
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        val poller = KtBridge.pollerNew(runtime)
        try {
            assertEquals(KtBridge.KT_ERR_WRONG_HANDLE_KIND, KtBridge.pollerFree(runtime))
            // The runtime must still be usable, i.e. the rejected free removed nothing.
            assertEquals(KtBridge.KT_OK, KtBridge.pollerWake(poller))
        } finally {
            KtBridge.pollerFree(poller)
            KtBridge.runtimeFree(runtime)
        }
    }

    @Test
    fun `polling after the runtime is freed returns immediately instead of parking`() {
        // Regression: the close signal was one-shot, so the first post-shutdown poll consumed it
        // and every later one blocked forever with no producer left alive. A real pump loops.
        val runtime = KtBridge.runtimeNew(ByteArray(0))
        val poller = KtBridge.pollerNew(runtime)
        KtBridge.runtimeFree(runtime)

        val done =
            Thread.ofPlatform().start {
                Arena.ofConfined().use { arena ->
                    val batch = arena.allocate(KtBridge.RECORD_BYTES * 4, 8)
                    val count = arena.allocate(JAVA_INT)
                    repeat(3) { KtBridge.poll(poller, batch, 4, -1, count) }
                }
            }
        done.join(5_000)
        assertTrue(!done.isAlive, "a pump looping after shutdown must not park")
        KtBridge.pollerFree(poller)
    }
}
