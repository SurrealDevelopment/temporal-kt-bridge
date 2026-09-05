//! Buffered Core metrics for replay through the application's JVM meter.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::{KeyValue, Value};
use parking_lot::Mutex;
use temporalio_common::telemetry::metrics::*;

use crate::proto::{self, metric_attribute, metric_update};

const MAX_UPDATES: usize = 65_536;

#[derive(Debug, Default)]
struct State {
    instruments: HashMap<(String, u32), u64>,
    definitions: Vec<proto::MetricDefinition>,
    updates: VecDeque<Sample>,
    dropped: u64,
}

#[derive(Debug)]
struct Sample {
    instrument: u64,
    attributes: MetricAttributes,
    value: metric_update::Value,
}

#[derive(Debug, Clone, Default)]
pub struct BridgeMeter {
    state: Arc<Mutex<State>>,
}

impl BridgeMeter {
    fn instrument(&self, kind: u32, params: MetricParameters) -> Instrument {
        let mut state = self.state.lock();
        let key = (params.name.to_string(), kind);
        let id = if let Some(id) = state.instruments.get(&key) {
            *id
        } else {
            let id = state.definitions.len() as u64 + 1;
            state.definitions.push(proto::MetricDefinition {
                id,
                name: params.name.into_owned(),
                description: params.description.into_owned(),
                unit: params.unit.into_owned(),
                kind,
            });
            state.instruments.insert(key, id);
            id
        };
        Instrument {
            state: self.state.clone(),
            id,
            attributes: MetricAttributes::Empty,
        }
    }

    pub fn drain(&self) -> proto::MetricBatch {
        let mut state = self.state.lock();
        let samples: Vec<_> = state.updates.drain(..).collect();
        let ids: BTreeSet<_> = samples.iter().map(|sample| sample.instrument).collect();
        let definitions = ids
            .into_iter()
            .map(|id| state.definitions[id as usize - 1].clone())
            .collect();
        let dropped = std::mem::take(&mut state.dropped);
        drop(state);
        let updates = samples
            .into_iter()
            .map(|sample| proto::MetricUpdate {
                instrument: sample.instrument,
                attributes: attributes_to_proto(&sample.attributes),
                value: Some(sample.value),
            })
            .collect();
        let logs = if dropped == 0 {
            vec![]
        } else {
            vec![proto::LogRecord {
                target: "temporal_kt_bridge::metrics".into(),
                level: "WARN".into(),
                message: format!(
                    "Dropped {dropped} oldest metric updates because the JVM did not drain the buffer in time"
                ),
                fields_json: String::new(),
            }]
        };
        proto::MetricBatch {
            definitions,
            updates,
            logs,
        }
    }
}

impl CoreMeter for BridgeMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        self.extend_attributes(MetricAttributes::Empty, attribs)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        let mut kvs = match existing {
            MetricAttributes::OTel { kvs } => (*kvs).clone(),
            _ => Vec::new(),
        };
        for attribute in attribs.attributes {
            let attribute: KeyValue = attribute.into();
            kvs.retain(|existing| existing.key != attribute.key);
            kvs.push(attribute);
        }
        // OTel attributes retain typed values and let Core's heartbeat gauges find label values.
        MetricAttributes::OTel { kvs: Arc::new(kvs) }
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        Counter::new(Arc::new(self.instrument(0, params)))
    }
    fn gauge(&self, params: MetricParameters) -> Gauge {
        Gauge::new(Arc::new(self.instrument(1, params)))
    }
    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        GaugeF64::new(Arc::new(self.instrument(2, params)))
    }
    fn histogram(&self, params: MetricParameters) -> Histogram {
        Histogram::new(Arc::new(self.instrument(3, params)))
    }
    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        HistogramF64::new(Arc::new(self.instrument(4, params)))
    }
    fn histogram_duration(&self, mut params: MetricParameters) -> HistogramDuration {
        params.unit = "ms".into();
        HistogramDuration::new(Arc::new(self.instrument(5, params)))
    }
    fn up_down_counter(&self, params: MetricParameters) -> UpDownCounter {
        UpDownCounter::new(Arc::new(self.instrument(6, params)))
    }
}

#[derive(Clone)]
struct Instrument {
    state: Arc<Mutex<State>>,
    id: u64,
    attributes: MetricAttributes,
}

impl Instrument {
    fn record(&self, value: metric_update::Value) {
        let mut state = self.state.lock();
        // ponytail: keep 65,536 updates; drain more frequently if samples are dropped. Definitions
        // live separately and never disappear when this queue overflows.
        if state.updates.len() == MAX_UPDATES {
            state.updates.pop_front();
            state.dropped += 1;
        }
        state.updates.push_back(Sample {
            instrument: self.id,
            attributes: self.attributes.clone(),
            value,
        });
    }
}

macro_rules! instrument {
    ($base:ident, $method:ident, $value:ty, $encode:expr) => {
        impl MetricAttributable<Box<dyn $base>> for Instrument {
            fn with_attributes(
                &self,
                attributes: &MetricAttributes,
            ) -> Result<Box<dyn $base>, Box<dyn std::error::Error>> {
                Ok(Box::new(Self {
                    attributes: attributes.clone(),
                    ..self.clone()
                }))
            }
        }
        impl $base for Instrument {
            fn $method(&self, value: $value) {
                self.record(($encode)(value));
            }
        }
    };
}

fn unsigned(value: u64) -> metric_update::Value {
    metric_update::Value::IntValue(value.min(i64::MAX as u64) as i64)
}

instrument!(CounterBase, adds, u64, unsigned);
instrument!(GaugeBase, records, u64, unsigned);
instrument!(
    GaugeF64Base,
    records,
    f64,
    metric_update::Value::DoubleValue
);
instrument!(HistogramBase, records, u64, unsigned);
instrument!(
    HistogramF64Base,
    records,
    f64,
    metric_update::Value::DoubleValue
);
instrument!(
    HistogramDurationBase,
    records,
    Duration,
    |value: Duration| metric_update::Value::IntValue(value.as_millis().min(i64::MAX as u128) as i64)
);
instrument!(UpDownCounterBase, adds, i64, metric_update::Value::IntValue);

fn attributes_to_proto(attributes: &MetricAttributes) -> Vec<proto::MetricAttribute> {
    let MetricAttributes::OTel { kvs } = attributes else {
        return vec![];
    };
    kvs.iter()
        .map(|attribute| proto::MetricAttribute {
            key: attribute.key.as_str().into(),
            value: Some(match &attribute.value {
                Value::String(value) => metric_attribute::Value::StringValue(value.as_str().into()),
                Value::I64(value) => metric_attribute::Value::IntValue(*value),
                Value::F64(value) => metric_attribute::Value::DoubleValue(*value),
                Value::Bool(value) => metric_attribute::Value::BoolValue(*value),
                value => metric_attribute::Value::StringValue(value.to_string()),
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn overflow_keeps_definitions_and_subsequent_updates_usable() {
        let meter = BridgeMeter::default();
        let counter = meter.counter("requests".into());
        let attrs = meter.new_attributes(NewAttributes::default());
        for _ in 0..MAX_UPDATES + 3 {
            counter.add(1, &attrs);
        }
        let batch = meter.drain();
        assert_eq!(batch.updates.len(), MAX_UPDATES);
        assert_eq!(batch.definitions.len(), 1);
        assert!(
            batch
                .updates
                .iter()
                .all(|sample| sample.instrument == batch.definitions[0].id)
        );
        assert!(batch.logs[0].message.contains("Dropped 3"));
        meter.counter("requests".into()).add(2, &attrs);
        let next = meter.drain();
        assert_eq!(next.definitions, batch.definitions);
        assert_eq!(
            next.updates[0].value,
            Some(metric_update::Value::IntValue(2))
        );
    }

    #[test]
    fn attributes_keep_types_override_values_and_support_heartbeat_labels() {
        let meter = BridgeMeter::default();
        let base = meter.new_attributes(vec![MetricKeyValue::new("poller_type", "old")].into());
        let attrs = meter.extend_attributes(
            base.clone(),
            vec![
                MetricKeyValue::new("poller_type", "workflow_task"),
                MetricKeyValue::new("attempt", 4_i64),
                MetricKeyValue::new("ratio", 0.5_f64),
                MetricKeyValue::new("sticky", true),
            ]
            .into(),
        );
        let current = Arc::new(AtomicU64::new(0));
        let gauge = meter.gauge_with_in_memory(
            "slots".into(),
            HeartbeatMetricType::WithLabel {
                label_key: "poller_type".into(),
                metrics: HashMap::from([("workflow_task".into(), current.clone())]),
            },
        );
        gauge.record(7, &attrs);
        assert_eq!(current.load(Ordering::Relaxed), 7);
        assert_eq!(
            attributes_to_proto(&base)[0].value,
            Some(metric_attribute::Value::StringValue("old".into()))
        );
        let batch = meter.drain();
        assert_eq!(batch.updates[0].attributes.len(), 4);
        assert_eq!(
            batch.updates[0].attributes[1].value,
            Some(metric_attribute::Value::IntValue(4))
        );
        assert_eq!(
            batch.updates[0].attributes[2].value,
            Some(metric_attribute::Value::DoubleValue(0.5))
        );
        assert_eq!(
            batch.updates[0].attributes[3].value,
            Some(metric_attribute::Value::BoolValue(true))
        );
    }

    #[test]
    fn instruments_preserve_kinds_signed_values_and_millisecond_durations() {
        let meter = BridgeMeter::default();
        let attrs = meter.new_attributes(NewAttributes::default());
        meter.counter("counter".into()).add(1, &attrs);
        meter.gauge("gauge".into()).record(2, &attrs);
        meter.gauge_f64("gauge-f64".into()).record(2.5, &attrs);
        meter.histogram("histogram".into()).record(3, &attrs);
        meter
            .histogram_f64("histogram-f64".into())
            .record(3.5, &attrs);
        meter
            .histogram_duration("duration".into())
            .record(Duration::from_micros(1_234_567), &attrs);
        meter.up_down_counter("up-down".into()).add(-2, &attrs);
        let batch = meter.drain();
        assert_eq!(
            batch
                .definitions
                .iter()
                .map(|def| def.kind)
                .collect::<Vec<_>>(),
            (0..7).collect::<Vec<_>>()
        );
        assert_eq!(batch.definitions[5].unit, "ms");
        assert_eq!(
            batch.updates[5].value,
            Some(metric_update::Value::IntValue(1234))
        );
        assert_eq!(
            batch.updates[6].value,
            Some(metric_update::Value::IntValue(-2))
        );
    }
}
