package com.surrealdev.temporal.core.internal

import com.surrealdev.temporal.core.proto.MetricAttribute
import com.surrealdev.temporal.core.proto.MetricBatch
import com.surrealdev.temporal.core.proto.MetricDefinition
import com.surrealdev.temporal.core.proto.MetricUpdate
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter

/** Replays Core's buffered observations into the application's existing Meter on a JVM thread. */
internal class CoreMetricsBridge(
    private val meter: Meter,
) {
    private val instruments = mutableMapOf<Long, (MetricUpdate, Attributes) -> Unit>()

    fun record(batch: MetricBatch) {
        batch.definitionsList.forEach { definition ->
            instruments.getOrPut(definition.id) { instrument(definition) }
        }
        batch.updatesList.forEach { update ->
            val attributes = Attributes.builder()
            update.attributesList.forEach { attribute ->
                when (attribute.valueCase) {
                    MetricAttribute.ValueCase.STRING_VALUE -> attributes.put(attribute.key, attribute.stringValue)
                    MetricAttribute.ValueCase.INT_VALUE -> attributes.put(attribute.key, attribute.intValue)
                    MetricAttribute.ValueCase.DOUBLE_VALUE -> attributes.put(attribute.key, attribute.doubleValue)
                    MetricAttribute.ValueCase.BOOL_VALUE -> attributes.put(attribute.key, attribute.boolValue)
                    else -> Unit
                }
            }
            instruments[update.instrument]?.invoke(update, attributes.build())
        }
    }

    private fun instrument(definition: MetricDefinition): (MetricUpdate, Attributes) -> Unit {
        val name = definition.name
        val description = definition.description
        val unit = definition.unit
        return when (definition.kind) {
            0 -> {
                val counter =
                    meter
                        .counterBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .build()
                val record: (
                    MetricUpdate,
                    Attributes,
                ) -> Unit = { update, attributes -> counter.add(update.intValue, attributes) }
                record
            }
            1 -> {
                val gauge =
                    meter
                        .gaugeBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .ofLongs()
                        .build()
                val record: (
                    MetricUpdate,
                    Attributes,
                ) -> Unit = { update, attributes -> gauge.set(update.intValue, attributes) }
                record
            }
            2 -> {
                val gauge =
                    meter
                        .gaugeBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .build()
                val record: (
                    MetricUpdate,
                    Attributes,
                ) -> Unit = { update, attributes -> gauge.set(update.doubleValue, attributes) }
                record
            }
            3, 5 -> {
                val histogram =
                    meter
                        .histogramBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .ofLongs()
                        .build()
                val record: (
                    MetricUpdate,
                    Attributes,
                ) -> Unit = { update, attributes -> histogram.record(update.intValue, attributes) }
                record
            }
            4 -> {
                val histogram =
                    meter
                        .histogramBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .build()
                val record: (
                    MetricUpdate,
                    Attributes,
                ) -> Unit = { update, attributes -> histogram.record(update.doubleValue, attributes) }
                record
            }
            6 -> {
                val counter =
                    meter
                        .upDownCounterBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .build()
                val record: (
                    MetricUpdate,
                    Attributes,
                ) -> Unit = { update, attributes -> counter.add(update.intValue, attributes) }
                record
            }
            else -> error("Unknown Core metric kind ${definition.kind}")
        }
    }
}
