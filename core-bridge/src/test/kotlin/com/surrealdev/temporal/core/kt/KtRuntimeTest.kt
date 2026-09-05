package com.surrealdev.temporal.core.kt

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class KtRuntimeTest {
    @Test
    fun `runtime closes registered streams and rejects late registration`() {
        KtRuntime.create().use { runtime ->
            val events = mutableListOf<Completion>()
            runtime.onWorkerEvents(123, events::add)
            runtime.close()
            assertEquals(listOf(0L, 1L, 2L), events.map { it.aux1 })
            assertEquals(listOf(Kind.TASK_STREAM_END), events.map { it.kind }.distinct())
            assertFailsWith<IllegalStateException> { runtime.onWorkerEvents(456) {} }
        }
    }
}
