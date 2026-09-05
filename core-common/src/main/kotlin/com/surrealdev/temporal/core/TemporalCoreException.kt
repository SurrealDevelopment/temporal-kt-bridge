package com.surrealdev.temporal.core

/**
 * Exception thrown when an error occurs in the Temporal Core native bridge.
 *
 * This exception wraps errors that originate from the Rust side of the FFI boundary.
 *
 * @param writableStackTrace Whether to capture a stack trace at construction.
 */
class TemporalCoreException(
    message: String,
    val errorType: String? = null,
    val statusCode: Int? = null,
    cause: Throwable? = null,
    writableStackTrace: Boolean = true,
    /** The server's grpc-status-details-bin bytes, if this was a gRPC rejection. */
    val details: ByteArray? = null,
) : RuntimeException(message, cause, true, writableStackTrace) {
    override fun toString(): String =
        buildString {
            append("TemporalCoreException")
            if (errorType != null) append("[$errorType]")
            append(": $message")
            if (statusCode != null) append(" (code=$statusCode)")
        }
}
