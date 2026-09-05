package com.surrealdev.temporal.core.kt

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Dev-server lifecycle from Kotlin, and a real client connected to the server it started.
 *
 * Needs a `temporal` binary; set TEMPORAL_CLI_PATH, or it is skipped loudly.
 */
class KtEphemeralServerTest {
    private fun cli(): String? = System.getenv("TEMPORAL_CLI_PATH")?.takeIf { it.isNotEmpty() }

    @Test
    fun `a dev server starts, serves a client, and shuts down`() {
        val path =
            cli() ?: run {
                println("skipping: set TEMPORAL_CLI_PATH to a temporal binary to run this")
                return
            }

        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(120_000) {
                    val server = KtEphemeralServer.start(runtime, serverOptions(path))
                    server.use {
                        assertTrue(server.pid > 0, "the pid must be reported without a fork patch")
                        assertTrue(server.target.isNotEmpty(), "the target must be reported")
                        // The child must not hold this JVM's stdio; that is what hangs a build.
                        assertTrue(isAlive(server.pid), "pid ${server.pid} should be a live process")

                        // The strongest check: the server it started actually serves.
                        KtClient.connect(runtime, clientOptions(server.target)).use { client ->
                            val response =
                                client.call(
                                    KtService.WORKFLOW,
                                    "DescribeNamespace",
                                    io.temporal.api.workflowservice.v1.DescribeNamespaceRequest
                                        .newBuilder()
                                        .setNamespace("default")
                                        .build()
                                        .toByteArray(),
                                )
                            assertEquals(
                                "default",
                                io.temporal.api.workflowservice.v1.DescribeNamespaceResponse
                                    .parseFrom(response)
                                    .namespaceInfo.name,
                            )
                        }

                        server.shutdown()
                        // The pid stays readable after shutdown; the C bridge read the live child
                        // and could not.
                        assertTrue(server.pid > 0)
                    }
                }
            }
        }
    }

    @Test
    fun `closing the runtime frees delivered servers clients and workers`() {
        val path =
            cli() ?: run {
                println("skipping: set TEMPORAL_CLI_PATH to a temporal binary to run this")
                return
            }
        KtRuntime.create().use { runtime ->
            runBlocking {
                withTimeout(120_000) {
                    KtEphemeralServer.start(runtime, serverOptions(path)).use { server ->
                        val child = ProcessHandle.of(server.pid.toLong()).orElseThrow()
                        KtClient.connect(runtime, clientOptions(server.target)).use { client ->
                            val options =
                                com.surrealdev.temporal.core.proto.WorkerOptions
                                    .newBuilder()
                                    .setNamespace("default")
                                    .setTaskQueue("runtime-cascade")
                                    .build()
                            KtWorker.create(runtime, client, options.toByteArray()).use { worker ->
                                worker.start()
                                runtime.close()
                                assertEquals(KtBridge.KT_ERR_STALE_HANDLE, KtBridge.clientFree(client.handle))
                                assertEquals(KtBridge.KT_ERR_STALE_HANDLE, KtBridge.workerFree(worker.handle))
                                assertEquals(KtBridge.KT_ERR_STALE_HANDLE, KtBridge.ephemeralFree(server.handle))
                                assertTrue(server.runtimeClosed)
                                child.onExit().get(5, java.util.concurrent.TimeUnit.SECONDS)
                                assertFalse(child.isAlive, "runtime close must stop its delivered server")
                            }
                        }
                    }
                }
            }
        }
    }

    private fun isAlive(pid: Int): Boolean = ProcessHandle.of(pid.toLong()).map { it.isAlive }.orElse(false)

    /** `kt_bridge.EphemeralServerOptions`: field 1 existing_path, field 6 namespace. */
    private fun serverOptions(path: String): ByteArray {
        val exe = path.toByteArray(Charsets.UTF_8)
        val ns = "default".toByteArray(Charsets.UTF_8)
        return byteArrayOf(0x0A, exe.size.toByte()) + exe + byteArrayOf(0x32, ns.size.toByte()) + ns
    }

    private fun clientOptions(target: String): ByteArray {
        val url = "http://$target".toByteArray(Charsets.UTF_8)
        val ns = "default".toByteArray(Charsets.UTF_8)
        return byteArrayOf(0x0A, url.size.toByte()) + url + byteArrayOf(0x12, ns.size.toByte()) + ns
    }
}
