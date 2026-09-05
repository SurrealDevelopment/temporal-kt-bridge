//! Dev-server lifecycle, covering the two things the C bridge worked around instead of fixing.
//!
//! Needs a `temporal` binary; set TEMPORAL_CLI_PATH, or it is skipped loudly.

use std::time::Duration;

use kt_bridge::abi::{KT_OK, KtCompletion, KtKind};
use kt_bridge::handle::{Entry, HANDLES};
use kt_bridge::{ephemeral, proto, queue, runtime};

fn cli() -> Option<String> {
    std::env::var("TEMPORAL_CLI_PATH")
        .ok()
        .filter(|p| !p.is_empty())
}

fn wait_for(poller: &queue::PollerEntry, timeout: Duration, want: u64) -> Option<KtCompletion> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let mut out = vec![
            KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0
            };
            16
        ];
        let n = unsafe { poller.poll(out.as_mut_ptr(), 16, 250) }.expect("poll");
        for record in &out[..n as usize] {
            if record.req_id == want {
                return Some(*record);
            }
        }
    }
    None
}

#[test]
fn a_dev_server_starts_reports_its_pid_and_shuts_down() {
    let Some(path) = cli() else {
        eprintln!("skipping: set TEMPORAL_CLI_PATH to a temporal binary to run this");
        return;
    };

    let rt = runtime::new_runtime(proto::RuntimeOptions::default()).expect("runtime");
    let poller = rt.queue.poller();

    let options = proto::EphemeralServerOptions {
        existing_path: path,
        namespace: "default".into(),
        // Discarded rather than inherited. Inheriting is what let an orphaned server hold a dead
        // JVM's stdout and hang an entire Gradle build with every test green.
        log_file: String::new(),
        ..Default::default()
    };

    runtime::spawn_request(&rt, 1, async move {
        match ephemeral::start(options).await {
            Ok(server) => {
                let info = server.info();
                let handle = HANDLES.insert(Entry::Ephemeral(server));
                queue::Pending::ack(1)
                    .kind(KtKind::EphemeralStarted)
                    .aux0(handle)
                    .payload(prost::Message::encode_to_vec(&info))
            }
            Err(message) => queue::Pending::error(1, -8, message),
        }
    })
    .expect("request accepted");

    let started = wait_for(&poller, Duration::from_secs(60), 1).expect("server never started");
    assert_eq!(started.status, KT_OK, "dev server failed to start");

    let bytes = unsafe {
        std::slice::from_raw_parts(started.payload as *const u8, started.payload_len as usize)
    };
    let info: proto::EphemeralServerInfo = prost::Message::decode(bytes).expect("info");

    // The pid is the whole reason the fork carried a C-bridge patch. It is public Rust API, so
    // reading it here needs no patch at all.
    eprintln!("started: pid={} target={}", info.pid, info.target);
    // Prove the pid names a real live process, not just a non-zero number.
    let alive = std::process::Command::new("ps")
        .args(["-p", &info.pid.to_string()])
        .output()
        .expect("ps");
    eprintln!("ps -p {} exit={}", info.pid, alive.status);
    assert!(
        alive.status.success(),
        "reported pid {} is not a live process",
        info.pid
    );
    assert!(info.pid > 0, "pid must be reported without any fork patch");
    assert!(!info.target.is_empty(), "target must be reported");

    // Still readable after shutdown, unlike the C bridge's accessor which read the live child and
    // was documented as unsafe to call concurrently with shutdown.
    let handle = started.aux0;
    let entry = HANDLES.ephemeral(handle).expect("handle");

    runtime::spawn_request(&rt, 2, async move {
        let finished = std::sync::atomic::AtomicBool::new(false);
        let (first, second) = tokio::join!(
            biased;
            async {
                let result = entry.shutdown().await;
                finished.store(true, std::sync::atomic::Ordering::Release);
                result
            },
            async {
                let result = entry.shutdown().await;
                assert!(
                    finished.load(std::sync::atomic::Ordering::Acquire),
                    "concurrent shutdown must wait for the child to be reaped"
                );
                result
            },
        );
        match first.and(second) {
            Ok(()) => queue::Pending::ack(2),
            Err(message) => queue::Pending::error(2, -8, message),
        }
    })
    .expect("request accepted");
    let stopped = wait_for(&poller, Duration::from_secs(30), 2).expect("shutdown never completed");
    assert_eq!(stopped.status, KT_OK, "shutdown reported an error");

    let after = HANDLES.ephemeral(handle).expect("handle still valid");
    assert_eq!(after.info().pid, info.pid, "pid must survive shutdown");

    runtime::free_runtime(rt);
}
