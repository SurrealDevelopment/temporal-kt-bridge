package com.surrealdev.temporal.core.kt

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith

class TaskStreamTest {
    @Test
    fun `cancellation after wakeup preserves tasks in order and closure drains them`(): Unit =
        runBlocking {
            val stream = TaskStream()
            val ready = java.util.ArrayDeque<Runnable>()
            val dispatcher =
                java.util.concurrent
                    .Executor { ready.add(it) }
                    .asCoroutineDispatcher()
            val receiver = async(dispatcher) { stream.receive() }
            ready.removeFirst().run()
            stream.send(byteArrayOf(1))
            stream.send(byteArrayOf(2))
            stream.close()
            receiver.cancel()
            ready.removeFirst().run()
            assertFailsWith<CancellationException> { receiver.await() }
            assertContentEquals(byteArrayOf(1), stream.receive())
            assertContentEquals(byteArrayOf(2), stream.receive())
            assertFailsWith<ClosedReceiveChannelException> { stream.receive() }
        }
}
