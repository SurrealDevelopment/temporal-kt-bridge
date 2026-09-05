package com.surrealdev.temporal.core

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The time-skipping test server, which had no coverage here at all.
 *
 * That gap let a wrong download version ship: the test server is a separate artifact from the
 * Temporal CLI and does not share its version line, so passing a CLI tag 404s on
 * temporal.download. Nothing in this module noticed, and every downstream integration test failed
 * at once.
 */
class TemporalTestServerTest {
    @Test
    fun `starts, skips time and shuts down`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                TemporalTestServer.start(runtime).use { server ->
                    assertTrue(server.targetUrl.isNotEmpty(), "the server must report a target")
                    assertNotNull(server.pid, "a running server has a pid")

                    // The point of this server: time is under the caller's control. The server
                    // starts with skipping locked -- an SDK unlocks it once it is waiting on
                    // something -- and Sleep with the lock held waits in real time, so the RPC
                    // would simply time out.
                    server.unlockTimeSkipping()
                    val before = server.getCurrentTime()
                    server.sleep(kotlin.time.Duration.parse("1h"))
                    val after = server.getCurrentTime()
                    assertTrue(
                        after.isAfter(before.plusSeconds(3000)),
                        "sleep(1h) should move the clock roughly an hour, was $before -> $after",
                    )
                    server.lockTimeSkipping()
                }
            }
        }

    @Test
    fun `pid is null once closed`() =
        runBlocking<Unit> {
            TemporalRuntime.create().use { runtime ->
                val server = TemporalTestServer.start(runtime)
                assertNotNull(server.pid)
                server.close()
                assertNull(server.pid, "a stopped server must not report a pid the OS can reuse")
            }
        }
}
