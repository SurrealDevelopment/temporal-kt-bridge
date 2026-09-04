package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException

/**
 * A dev or test server on the kt-bridge ABI.
 *
 * The child never inherits this JVM's stdout or stderr. That inheritance is what made a single
 * leaked server hang an entire Gradle build with every test green: Gradle waits for pipe EOF
 * after a test worker exits, and an orphaned child still holds those pipes. Output goes to a
 * named file or is discarded.
 *
 * [pid] needs no fork patch. `EphemeralServer::child_process_id()` is public Rust API and always
 * was; the C bridge simply did not expose it. It is captured at start, so it stays readable after
 * shutdown -- the C bridge read the live child and was documented as unsafe to call concurrently
 * with shutdown.
 */
internal class KtEphemeralServer private constructor(
    private val runtime: KtRuntime,
    val handle: Long,
    val target: String,
    val pid: Int,
    val hasTestService: Boolean,
) : AutoCloseable {
    @Volatile
    private var shutDown = false

    suspend fun shutdown() {
        if (shutDown) return
        val completion =
            runtime.pump.request { reqId ->
                KtBridge.ephemeralShutdown(runtime.handle, handle, reqId)
            }
        shutDown = true
        if (completion.isFailure) {
            throw TemporalCoreException(
                message = "could not shut down the ephemeral server: ${completion.errorMessage()}",
                errorType = null,
                statusCode = completion.status,
                cause = null,
                writableStackTrace = true,
            )
        }
    }

    override fun close() {
        // Rust also kills the child on drop, so a forgotten or unwound path cannot leak a server.
        KtBridge.ephemeralFree(handle)
    }

    companion object {
        /** @param config an encoded `kt_bridge.EphemeralServerOptions`. */
        suspend fun start(
            runtime: KtRuntime,
            config: ByteArray,
        ): KtEphemeralServer {
            runtime.ensureOpen()
            val completion =
                runtime.pump.request { reqId -> KtBridge.ephemeralStart(runtime.handle, config, reqId) }
            if (completion.isFailure) {
                throw TemporalCoreException(
                    message = "could not start the ephemeral server: ${completion.errorMessage()}",
                    errorType = null,
                    statusCode = completion.status,
                    cause = null,
                    writableStackTrace = true,
                )
            }
            val info = EphemeralServerInfo.parse(completion.payload)
            return KtEphemeralServer(
                runtime = runtime,
                handle = completion.aux0,
                target = info.target,
                pid = info.pid,
                hasTestService = info.hasTestService,
            )
        }
    }
}

/**
 * A hand-decoded `kt_bridge.EphemeralServerInfo`.
 *
 * Hand-decoded because the bridge's own protos are not on the SDK's classpath: `:protos` carries
 * Temporal's schema, not this crate's private config messages. Three fields does not justify a
 * second generated proto artifact.
 */
internal data class EphemeralServerInfo(
    val target: String,
    val pid: Int,
    val hasTestService: Boolean,
) {
    companion object {
        fun parse(bytes: ByteArray): EphemeralServerInfo {
            var index = 0
            var target = ""
            var pid = 0
            var hasTestService = false

            fun varint(): Long {
                var result = 0L
                var shift = 0
                while (index < bytes.size) {
                    val byte = bytes[index++].toInt()
                    result = result or ((byte and 0x7F).toLong() shl shift)
                    if (byte and 0x80 == 0) break
                    shift += 7
                }
                return result
            }

            while (index < bytes.size) {
                val tag = varint()
                when ((tag shr 3).toInt()) {
                    1 -> {
                        val length = varint().toInt()
                        target = String(bytes, index, length, Charsets.UTF_8)
                        index += length
                    }
                    2 -> pid = varint().toInt()
                    3 -> hasTestService = varint() != 0L
                    else ->
                        when ((tag and 0x7).toInt()) {
                            0 -> varint()
                            2 -> index += varint().toInt()
                            else -> return EphemeralServerInfo(target, pid, hasTestService)
                        }
                }
            }
            return EphemeralServerInfo(target, pid, hasTestService)
        }
    }
}
