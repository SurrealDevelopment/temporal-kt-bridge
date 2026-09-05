package com.surrealdev.temporal.core.internal

import com.surrealdev.temporal.core.ClientOptions
import com.surrealdev.temporal.core.TemporalCoreClient
import com.surrealdev.temporal.core.TemporalDevServer
import com.surrealdev.temporal.core.TemporalRuntime
import com.surrealdev.temporal.core.proto.MetricAttribute
import com.surrealdev.temporal.core.proto.MetricBatch
import com.surrealdev.temporal.core.proto.MetricDefinition
import com.surrealdev.temporal.core.proto.MetricUpdate
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.temporal.api.workflowservice.v1.GetSystemInfoRequest
import io.temporal.api.workflowservice.v1.GetSystemInfoResponse
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CoreMetricsBridgeTest {
    @Test
    fun `Core observations preserve kinds deltas and attributes`() {
        val reader = InMemoryMetricReader.create()
        SdkMeterProvider.builder().registerMetricReader(reader).build().use { provider ->
            val bridge = CoreMetricsBridge(provider.get("test"))
            val batch = MetricBatch.newBuilder()
            for (kind in 0..6) {
                batch.addDefinitions(
                    MetricDefinition
                        .newBuilder()
                        .setId(kind + 1L)
                        .setName("kind_$kind")
                        .setKind(kind)
                        .setUnit(if (kind == 5) "ms" else "1"),
                )
                val update =
                    MetricUpdate
                        .newBuilder()
                        .setInstrument(kind + 1L)
                        .addAttributes(MetricAttribute.newBuilder().setKey("string").setStringValue("value"))
                        .addAttributes(MetricAttribute.newBuilder().setKey("int").setIntValue(7))
                        .addAttributes(MetricAttribute.newBuilder().setKey("double").setDoubleValue(0.5))
                        .addAttributes(MetricAttribute.newBuilder().setKey("bool").setBoolValue(true))
                if (kind == 2 ||
                    kind == 4
                ) {
                    update.setDoubleValue(1.5)
                } else {
                    update.setIntValue(if (kind == 6) -2 else 42)
                }
                batch.addUpdates(update)
            }
            bridge.record(batch.build())
            val metrics = reader.collectAllMetrics().associateBy { it.name }
            assertEquals(7, metrics.size)
            assertEquals(
                42,
                metrics
                    .getValue("kind_0")
                    .longSumData.points
                    .single()
                    .value,
            )
            assertEquals(
                42,
                metrics
                    .getValue("kind_1")
                    .longGaugeData.points
                    .single()
                    .value,
            )
            assertEquals(
                1.5,
                metrics
                    .getValue("kind_2")
                    .doubleGaugeData.points
                    .single()
                    .value,
            )
            assertEquals(
                42.0,
                metrics
                    .getValue("kind_3")
                    .histogramData.points
                    .single()
                    .sum,
            )
            assertEquals(
                1.5,
                metrics
                    .getValue("kind_4")
                    .histogramData.points
                    .single()
                    .sum,
            )
            assertEquals("ms", metrics.getValue("kind_5").unit)
            assertEquals(
                42.0,
                metrics
                    .getValue("kind_5")
                    .histogramData.points
                    .single()
                    .sum,
            )
            assertEquals(
                -2,
                metrics
                    .getValue("kind_6")
                    .longSumData.points
                    .single()
                    .value,
            )
            val attributes =
                metrics
                    .getValue("kind_0")
                    .longSumData.points
                    .single()
                    .attributes
            assertEquals("value", attributes.get(AttributeKey.stringKey("string")))
            assertEquals(7, attributes.get(AttributeKey.longKey("int")))
            assertEquals(0.5, attributes.get(AttributeKey.doubleKey("double")))
            assertEquals(true, attributes.get(AttributeKey.booleanKey("bool")))
        }
    }

    @Test
    fun `Core RPC metrics reach the supplied meter`() =
        runBlocking {
            val reader = InMemoryMetricReader.create()
            SdkMeterProvider.builder().registerMetricReader(reader).build().use { provider ->
                TemporalRuntime.create(provider.get("application"), workerHeartbeatIntervalMs = 0).use { runtime ->
                    TemporalDevServer.start(runtime).use { server ->
                        TemporalCoreClient
                            .connect(
                                runtime,
                                server.targetUrl,
                                "default",
                                options = ClientOptions(),
                            ).use { client ->
                                client.workflowServiceCall("GetSystemInfo", GetSystemInfoRequest.getDefaultInstance()) {
                                    GetSystemInfoResponse.parseFrom(it)
                                }
                            }
                    }
                }
                val metrics = reader.collectAllMetrics()
                assertTrue(
                    metrics.any {
                        it.name == "temporal_request" &&
                            it.longSumData.points.any { point -> point.value > 0 }
                    },
                )
                assertTrue(
                    metrics.any { it.name == "temporal_request_latency" && it.histogramData.points.isNotEmpty() },
                )
            }
        }
}
