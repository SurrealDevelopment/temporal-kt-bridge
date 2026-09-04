package com.surrealdev.temporal.core.kt

import com.surrealdev.temporal.core.TemporalCoreException

/** The gRPC services the bridge can dispatch to. Must match `Service` in rpc.rs. */
internal enum class KtService(
    val code: Int,
) {
    WORKFLOW(0),
    OPERATOR(1),
    TEST(2),
}

/**
 * A connected client on the kt-bridge ABI.
 *
 * Every call is a suspending request answered by exactly one completion, so an ordinary coroutine
 * cancellation is enough to abandon one -- unlike the C bridge, where no native call could be
 * cancelled because Rust always fired its callback and held `Arc`s until it did.
 */
internal class KtClient(
    private val runtime: KtRuntime,
    val handle: Long,
) : AutoCloseable {
    /**
     * Makes one gRPC call and returns the encoded response.
     *
     * A gRPC error keeps the server's own status code, so callers can map it to the exception the
     * server intended rather than a generic failure.
     */
    suspend fun call(
        service: KtService,
        rpc: String,
        request: ByteArray,
    ): ByteArray {
        runtime.ensureOpen()
        val completion =
            runtime.pump.request { reqId ->
                KtBridge.clientRpc(runtime.handle, handle, service.code, rpc, request, reqId)
            }
        if (completion.isFailure) {
            throw TemporalCoreException(
                message = "$rpc failed: ${completion.errorMessage()}",
                errorType = null,
                statusCode = completion.status,
                cause = null,
                writableStackTrace = true,
            )
        }
        return completion.payload
    }

    override fun close() {
        KtBridge.clientFree(handle)
    }

    companion object {
        /** @param config an encoded `kt_bridge.ClientOptions`. */
        suspend fun connect(
            runtime: KtRuntime,
            config: ByteArray,
        ): KtClient {
            runtime.ensureOpen()
            val completion =
                runtime.pump.request { reqId -> KtBridge.clientConnect(runtime.handle, config, reqId) }
            if (completion.isFailure) {
                throw TemporalCoreException(
                    message = "could not connect: ${completion.errorMessage()}",
                    errorType = null,
                    statusCode = completion.status,
                    cause = null,
                    writableStackTrace = true,
                )
            }
            return KtClient(runtime, completion.aux0)
        }
    }
}
