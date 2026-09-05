package com.surrealdev.temporal.core

import com.google.protobuf.ByteString
import com.google.protobuf.CodedInputStream
import com.google.protobuf.MessageLite
import com.surrealdev.temporal.core.kt.KtClient
import com.surrealdev.temporal.core.kt.KtService
import org.slf4j.LoggerFactory
import com.surrealdev.temporal.core.proto.ClientOptions as ProtoClientOptions

/**
 * Transport-level gRPC compression for client connections.
 */
enum class GrpcCompression {
    /** Gzip-compress request bodies and accept gzip responses. Core's default. */
    GZIP,

    /** No transport compression. Use when an intermediary (proxy/gateway) rejects compressed frames. */
    NONE,
}

/**
 * Options for configuring a Temporal client connection.
 */
data class ClientOptions(
    val clientName: String = "temporal-kotlin",
    val clientVersion: String = BuildConfig.SDK_VERSION,
    val identity: String? = null,
    val grpcCompression: GrpcCompression = GrpcCompression.GZIP,
    val connectTimeoutMillis: Long = 0,
) {
    init {
        require(connectTimeoutMillis >= 0) { "connectTimeoutMillis must not be negative" }
    }
}

/**
 * A high-level wrapper for the Temporal Core client.
 *
 * This class manages the lifecycle of a client connection to a Temporal server.
 * It wraps the low-level FFM bindings and provides a coroutine-friendly API.
 *
 * Example usage:
 * ```kotlin
 * TemporalRuntime.create().use { runtime ->
 *     val client = TemporalCoreClient.connect(runtime, "localhost:7233", "default")
 *     try {
 *         // Use the client...
 *     } finally {
 *         client.close()
 *     }
 * }
 * ```
 *
 * A client connection to a Temporal server.
 *
 * Every call is one suspending request answered by exactly one completion from the bridge, so an
 * ordinary coroutine cancellation is enough to abandon one. The previous bridge could not cancel
 * a native call at all: Rust always fired its callback and held `Arc`s until it did.
 */
class TemporalCoreClient private constructor(
    internal val kt: KtClient,
    private val runtime: TemporalRuntime,
    val targetUrl: String,
    val namespace: String,
) : AutoCloseable {
    @Volatile
    private var closed = false

    companion object {
        private val logger = LoggerFactory.getLogger(TemporalCoreClient::class.java)

        /**
         * Connects to a Temporal server.
         *
         * @throws TemporalCoreException if the connection cannot be established
         */
        suspend fun connect(
            runtime: TemporalRuntime,
            targetUrl: String,
            namespace: String = "default",
            options: ClientOptions = ClientOptions(),
            tls: TlsConfig? = null,
            apiKey: String? = null,
            tlsDisabled: Boolean = false,
        ): TemporalCoreClient {
            runtime.ensureOpen()

            if (tlsDisabled && tls != null) {
                logger.warn("tlsDisabled=true but an explicit TLS config was provided; TLS will NOT be used.")
            }
            if (tlsDisabled && targetUrl.startsWith("https://", ignoreCase = true)) {
                logger.warn("tlsDisabled=true but the target URL is https://; TLS will NOT be used.")
            }

            val config = clientConfig(targetUrl, namespace, options, tls, apiKey, tlsDisabled)
            val normalizedUrl = config.targetUrl
            val client = KtClient.connect(runtime.kt, config.toByteArray())
            return TemporalCoreClient(client, runtime, normalizedUrl, namespace)
        }
    }

    fun isClosed(): Boolean = closed || runtime.isClosed()

    private fun ensureOpen() {
        check(!closed) { "Client has been closed" }
        runtime.ensureOpen()
    }

    /**
     * Calls a WorkflowService RPC.
     *
     * @param rpc the PascalCase method name, e.g. "StartWorkflowExecution"
     * @throws TemporalCoreException carrying the server's own gRPC status code on rejection
     */
    suspend fun <Req : MessageLite, Resp : MessageLite> workflowServiceCall(
        rpc: String,
        request: Req,
        timeoutMillis: Int = 0,
        parser: (CodedInputStream) -> Resp,
    ): Resp = call(KtService.WORKFLOW, rpc, request, timeoutMillis, parser)

    /** Calls a TestService RPC. Only available against a test server with time skipping. */
    suspend fun <Req : MessageLite, Resp : MessageLite> testServiceCall(
        rpc: String,
        request: Req,
        timeoutMillis: Int = 0,
        parser: (CodedInputStream) -> Resp,
    ): Resp = call(KtService.TEST, rpc, request, timeoutMillis, parser)

    private suspend fun <Req : MessageLite, Resp : MessageLite> call(
        service: KtService,
        rpc: String,
        request: Req,
        timeoutMillis: Int,
        parser: (CodedInputStream) -> Resp,
    ): Resp {
        ensureOpen()
        require(timeoutMillis >= 0) { "timeoutMillis must not be negative" }
        // Zero keeps Core's RPC defaults; a positive timeout bounds the call including retries.
        val response = kt.call(service, rpc, request.toByteArray(), timeoutMillis.toLong())
        return parser(CodedInputStream.newInstance(response))
    }

    override fun close() {
        if (closed) return
        synchronized(this) {
            if (closed) return
            closed = true
            kt.close()
        }
    }
}

/** Build the bridge config after resolving TLS consistently for every target spelling. */
internal fun clientConfig(
    targetUrl: String,
    namespace: String,
    options: ClientOptions = ClientOptions(),
    tls: TlsConfig? = null,
    apiKey: String? = null,
    tlsDisabled: Boolean = false,
): ProtoClientOptions {
    val https = targetUrl.startsWith("https://", ignoreCase = true)
    val address =
        if (https || targetUrl.startsWith("http://", ignoreCase = true)) targetUrl.substringAfter("://") else targetUrl
    val useTls = !tlsDisabled && (https || tls != null || apiKey != null)
    return ProtoClientOptions
        .newBuilder()
        .apply {
            this.targetUrl = "${if (useTls) "https" else "http"}://$address"
            this.namespace = namespace
            identity = options.identity.orEmpty()
            clientName = options.clientName
            clientVersion = options.clientVersion
            this.apiKey = apiKey.orEmpty()
            connectTimeoutMillis = options.connectTimeoutMillis
            noCompression = options.grpcCompression == GrpcCompression.NONE
            this.tls = useTls
            if (useTls && tls != null) {
                tls.serverRootCaCert?.let { serverRootCaCert = ByteString.copyFrom(it) }
                tls.domain?.let { tlsDomain = it }
                tls.clientCert?.let { clientCert = ByteString.copyFrom(it) }
                tls.clientPrivateKey?.let { clientPrivateKey = ByteString.copyFrom(it) }
            }
        }.build()
}
