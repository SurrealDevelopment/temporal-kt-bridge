package com.surrealdev.temporal.core

/**
 * An ephemeral Temporal development server.
 *
 * This server is useful for local development. It runs an in-memory Temporal
 * server that automatically downloads and starts the Temporal CLI dev server.
 * Time flows at real-time pace.
 *
 * For testing with time-skipping, use [TemporalTestServer] instead.
 *
 * Example usage:
 * ```kotlin
 * TemporalRuntime.create().use { runtime ->
 *     TemporalDevServer.start(runtime).use { server ->
 *         println("Dev server running at: ${server.targetUrl}")
 *     }
 * }
 * ```
 */
class TemporalDevServer private constructor(
    private val kt: com.surrealdev.temporal.core.kt.KtEphemeralServer,
) : EphemeralServer {
    @Volatile
    private var closed = false

    @Volatile
    private var shutDown = false

    override val targetUrl: String get() = kt.target

    /**
     * The OS pid of the server process, or null once it has been shut down.
     *
     * Null after [close] on purpose: the process is gone, and the OS is free to hand that number
     * to something else. A caller that reaped a remembered pid would eventually kill an unrelated
     * process. Exposing this at all needed a carried fork patch on the previous bridge, even
     * though `child_process_id()` has always been public Rust API.
     */
    override val pid: Long? get() =
        if (closed ||
            shutDown ||
            kt.runtimeClosed
        ) {
            null
        } else {
            kt.pid.takeIf { it > 0 }?.toLong()
        }

    override fun isClosed(): Boolean = closed || kt.runtimeClosed

    companion object {
        @Suppress("LongParameterList")
        suspend fun start(
            runtime: TemporalRuntime,
            namespace: String = "default",
            ip: String = "127.0.0.1",
            existingPath: String? = null,
            downloadVersion: String? = BuildConfig.TEMPORAL_CLI_VERSION,
            // Zero keeps cached downloads indefinitely.
            downloadTtlSeconds: Long = 0,
            searchAttributes: List<Pair<String, String>> = emptyList(),
            extraArgs: List<String> = emptyList(),
            logFile: String? = null,
        ): TemporalDevServer {
            runtime.ensureOpen()
            val allArgs =
                buildList {
                    searchAttributes.forEach { (name, type) ->
                        add("--search-attribute")
                        add("$name=$type")
                    }
                    addAll(extraArgs)
                }
            val server =
                com.surrealdev.temporal.core.kt.KtEphemeralServer.start(
                    runtime.kt,
                    EphemeralServerOptionsProto.encode(
                        existingPath = existingPath,
                        downloadVersion = downloadVersion,
                        downloadTtlSeconds = downloadTtlSeconds,
                        namespace = namespace,
                        ip = ip,
                        extraArgs = allArgs,
                        testServer = false,
                        logFile = logFile,
                    ),
                )
            return TemporalDevServer(server).also { EphemeralServers.register(it) }
        }
    }

    suspend fun shutdown() {
        if (closed) return
        shutDown = true
        kt.shutdown()
    }

    override fun close() {
        if (closed) return
        synchronized(this) {
            if (closed) return
            closed = true
            kt.close()
            EphemeralServers.unregister(this)
        }
    }
}
