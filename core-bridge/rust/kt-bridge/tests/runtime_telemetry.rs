use kt_bridge::{
    abi,
    handle::{Entry, HANDLES},
    proto, runtime,
};
use prost::Message;
use temporalio_common::telemetry::metrics::{CoreMeter, NewAttributes};

fn config() -> proto::RuntimeOptions {
    proto::RuntimeOptions {
        telemetry: Some(proto::TelemetryOptions {
            buffer_metrics: true,
            forward_logs: true,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn buffer_size_retry_preserves_the_original_batch_and_leaves_later_samples_for_next_drain() {
    let entry = runtime::new_runtime(config()).unwrap();
    let meter = entry.metrics.as_ref().unwrap();
    let counter = meter.counter("test".into());
    let attrs = meter.new_attributes(NewAttributes::default());
    counter.add(1, &attrs);
    let handle = HANDLES.insert(Entry::Runtime(entry.clone()));
    let mut length = 0;
    assert_eq!(
        unsafe {
            kt_bridge::kt_runtime_retrieve_metrics(handle, std::ptr::null_mut(), 0, &mut length)
        },
        abi::KT_ERR_BUFFER_TOO_SMALL
    );
    counter.add(2, &attrs);
    let mut bytes = vec![0; length as usize];
    assert_eq!(
        unsafe {
            kt_bridge::kt_runtime_retrieve_metrics(handle, bytes.as_mut_ptr(), length, &mut length)
        },
        abi::KT_OK
    );
    let first = proto::MetricBatch::decode(bytes.as_slice()).unwrap();
    assert_eq!(first.updates.len(), 1);
    assert_eq!(
        first.updates[0].value,
        Some(proto::metric_update::Value::IntValue(1))
    );
    let next = runtime::drain_metrics(&entry);
    assert_eq!(next.updates.len(), 1);
    assert_eq!(
        next.updates[0].value,
        Some(proto::metric_update::Value::IntValue(2))
    );
    assert_eq!(next.definitions, first.definitions);
    assert_eq!(unsafe { kt_bridge::kt_runtime_free(handle) }, abi::KT_OK);
}

#[test]
fn runtime_forwards_filtered_logs_and_reports_the_locked_core_version() {
    let entry = runtime::new_runtime(config()).unwrap();
    entry.core.tokio_handle().block_on(async {
        tokio::spawn(async {
            tracing::warn!(test_value = 42, "bridge-warning");
            tracing::info!("bridge-info");
        })
        .await
        .unwrap();
    });
    let batch = runtime::drain_metrics(&entry);
    let warning = batch
        .logs
        .iter()
        .find(|log| log.message == "bridge-warning")
        .unwrap();
    assert_eq!(warning.level, "WARN");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&warning.fields_json).unwrap()["test_value"],
        42
    );
    assert!(!batch.logs.iter().any(|log| log.message == "bridge-info"));
    assert_eq!(runtime::runtime_info(&entry).core_version, "0.8.0");
}

#[test]
fn heartbeat_zero_is_disabled_and_nonzero_intervals_follow_core_limits() {
    for millis in [None, Some(0), Some(1_000), Some(60_000)] {
        let entry = runtime::new_runtime(proto::RuntimeOptions {
            worker_heartbeat_interval_millis: millis,
            ..Default::default()
        })
        .unwrap();
        drop(entry);
    }
    for millis in [1, 999, 60_001] {
        assert!(matches!(
            runtime::new_runtime(proto::RuntimeOptions {
                worker_heartbeat_interval_millis: Some(millis),
                ..Default::default()
            }),
            Err(kt_bridge::error::KtError::InvalidArgument(_))
        ));
    }
}
