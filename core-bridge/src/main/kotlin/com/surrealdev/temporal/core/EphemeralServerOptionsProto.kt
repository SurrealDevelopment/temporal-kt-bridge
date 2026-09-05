package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.proto.EphemeralServerOptions

/** Encodes the shared server config, including the caller's download cache lifetime. */
internal object EphemeralServerOptionsProto {
    fun encode(
        existingPath: String?,
        downloadVersion: String?,
        downloadTtlSeconds: Long,
        namespace: String,
        ip: String,
        extraArgs: List<String>,
        testServer: Boolean,
        logFile: String?,
    ): ByteArray {
        require(downloadTtlSeconds >= 0) { "downloadTtlSeconds must be nonnegative" }
        return EphemeralServerOptions
            .newBuilder()
            .setExistingPath(existingPath.orEmpty())
            .setDownloadVersion(downloadVersion.orEmpty())
            .setDownloadTtlSeconds(downloadTtlSeconds)
            .setNamespace(namespace)
            .setIp(ip)
            .addAllExtraArgs(extraArgs)
            .setTestServer(testServer)
            .setLogFile(logFile.orEmpty())
            .build()
            .toByteArray()
    }
}
