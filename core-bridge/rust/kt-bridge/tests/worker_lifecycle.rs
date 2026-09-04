//! End-to-end worker lifecycle against a real Temporal dev server.
//!
//! Unit tests show the queue and handle table behave in isolation. Only a real server shows the
//! thing this bridge is actually claiming: that the Rust-owned poll loops reach Core, that
//! shutdown observes `PollError::ShutDown` on every stream, and that the worker then finalizes
//! instead of hanging. Hanging shutdown is the specific failure the C bridge kept producing, so
//! it is the specific thing worth proving.
//!
//! Requires a dev server. Set `TEMPORAL_TEST_ADDRESS` (e.g. `localhost:7233`); skipped otherwise,
//! because a test that silently passes when it cannot run is worse than no test.

use std::time::Duration;

use kt_bridge::abi::{KT_OK, KtCompletion, KtKind};
use kt_bridge::handle::{Entry, HANDLES};
use kt_bridge::{client, proto, queue, runtime, worker};

fn server_address() -> Option<String> {
    std::env::var("TEMPORAL_TEST_ADDRESS").ok().filter(|s| !s.is_empty())
}

fn empty_batch(n: usize) -> Vec<KtCompletion> {
    vec![
        KtCompletion { req_id: 0, kind: 0, status: 0, payload: 0, payload_len: 0, aux0: 0, aux1: 0 };
        n
    ]
}

/// Drains completions until `want` matches one, or the deadline passes.
fn wait_for(
    poller: &queue::PollerEntry,
    timeout: Duration,
    mut want: impl FnMut(&KtCompletion) -> bool,
) -> Option<KtCompletion> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let mut out = empty_batch(32);
        let n = unsafe { poller.poll(out.as_mut_ptr(), 32, 200) }.expect("poll");
        for record in &out[..n as usize] {
            if want(record) {
                return Some(*record);
            }
        }
    }
    None
}

#[test]
fn a_worker_starts_polls_and_shuts_down_without_hanging() {
    let Some(address) = server_address() else {
        eprintln!("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server");
        return;
    };
    eprintln!("running against {address}");

    let rt = runtime::new_runtime(proto::RuntimeOptions::default()).expect("runtime");
    let poller = rt.queue.poller();

    // Connect.
    let options = client::connection_options(&proto::ClientOptions {
        target_url: format!("http://{address}"),
        namespace: "default".into(),
        identity: "kt-bridge-integration".into(),
        ..Default::default()
    })
    .expect("connection options");

    runtime::spawn_request(&rt, 1, {
        let options = options.clone();
        async move {
            match client::connect(options, "default".into()).await {
                Ok(c) => {
                    let handle = HANDLES.insert(Entry::Client(c));
                    queue::Pending::ack(1).kind(KtKind::ClientConnected).aux0(handle)
                }
                Err(message) => queue::Pending::error(1, -8, message),
            }
        }
    });

    let connected = wait_for(&poller, Duration::from_secs(20), |r| r.req_id == 1)
        .expect("client connect produced no completion");
    assert_eq!(connected.status, KT_OK, "could not connect to {address}");
    let client_handle = connected.aux0;

    // Build and start a worker.
    let cl = HANDLES.client(client_handle).expect("client handle");
    let config = worker::worker_config(&proto::WorkerOptions {
        namespace: "default".into(),
        task_queue: "kt-bridge-integration".into(),
        identity: "kt-bridge-integration".into(),
        ..Default::default()
    })
    .expect("worker config");

    let core = {
        let _entered = rt.core.tokio_handle().enter();
        temporalio_sdk_core::init_worker(&rt.core, config, cl.connection.clone()).expect("init_worker")
    };
    let entry = std::sync::Arc::new(worker::WorkerEntry::new(std::sync::Arc::new(core), rt.sender()));
    let worker_handle = HANDLES.insert(Entry::Worker(entry.clone()));

    worker::start(&entry, &rt, worker_handle).expect("start");

    // Shut down and require that every stream ends and the worker finalizes. If the poll loops
    // did not reach Core, or shutdown finalized before they saw ShutDown, this is where the old
    // bridge hung.
    runtime::spawn_request(&rt, 2, {
        let entry = entry.clone();
        async move {
            match worker::shutdown(entry, Duration::from_secs(20)).await {
                Ok(()) => queue::Pending::ack(2),
                Err(message) => queue::Pending::error(2, -8, message),
            }
        }
    });

    let mut stream_ends = 0;
    let done = wait_for(&poller, Duration::from_secs(30), |r| {
        if r.kind == KtKind::TaskStreamEnd as u32 {
            stream_ends += 1;
        }
        r.req_id == 2
    });

    let done = done.expect("worker shutdown never completed - this is the hang being tested for");
    eprintln!("shutdown status={} stream_ends={}", done.status, stream_ends);
    assert_eq!(done.status, KT_OK, "shutdown reported an error");
    assert_eq!(stream_ends, 3, "every poll stream must report ShutDown exactly once");

    runtime::free_runtime(rt);
}
