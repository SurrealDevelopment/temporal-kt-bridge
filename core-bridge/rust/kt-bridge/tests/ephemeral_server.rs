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

    let tokio = rt.core.tokio_handle().clone();
    let tasks = rt.tasks.clone();
    runtime::spawn_request(&rt, 1, async move {
        match ephemeral::start(options, tokio, tasks).await {
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

#[test]
#[cfg(unix)]
fn runtime_close_reaps_a_completed_start_discarded_on_a_runtime_thread() {
    let Some(path) = cli() else {
        eprintln!("skipping: set TEMPORAL_CLI_PATH to a temporal binary to run this");
        return;
    };
    let rt = runtime::new_runtime(proto::RuntimeOptions::default()).expect("runtime");
    let server = rt
        .core
        .tokio_handle()
        .block_on(ephemeral::start(
            proto::EphemeralServerOptions {
                existing_path: path,
                ..Default::default()
            },
            rt.core.tokio_handle().clone(),
            rt.tasks.clone(),
        ))
        .expect("server");
    let pid = server.pid;
    let handle = HANDLES.insert(Entry::Ephemeral(server));
    let abandoned = queue::Pending::ack(1)
        .kind(KtKind::EphemeralStarted)
        .aux0(handle);
    runtime::spawn_request(&rt, 1, async move {
        let _owned = abandoned;
        std::future::pending().await
    })
    .expect("request accepted");
    // Cancellation drops Pending on Tokio, where blocking to reap a child would panic.
    runtime::free_runtime(rt);
    assert!(HANDLES.ephemeral(handle).is_err());
    assert_reaped(pid);
}

#[cfg(unix)]
fn assert_reaped(pid: u32) {
    let process = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    assert!(
        process.stdout.is_empty(),
        "child {pid} must already be reaped, state={}",
        String::from_utf8_lossy(&process.stdout),
    );
}

#[test]
#[cfg(unix)]
fn runtime_close_waits_for_a_removed_server_before_cleanup_is_scheduled() {
    let Some(path) = cli() else {
        eprintln!("skipping: set TEMPORAL_CLI_PATH to a temporal binary to run this");
        return;
    };
    let rt = runtime::new_runtime(proto::RuntimeOptions::default()).expect("runtime");
    let server = rt
        .core
        .tokio_handle()
        .block_on(ephemeral::start(
            proto::EphemeralServerOptions {
                existing_path: path,
                ..Default::default()
            },
            rt.core.tokio_handle().clone(),
            rt.tasks.clone(),
        ))
        .expect("server");
    let pid = server.pid;
    let handle = HANDLES.insert(Entry::Ephemeral(server));
    // Pause free exactly after table removal. The reactor must still belong to this live child.
    let removed = HANDLES
        .remove_of_kind(handle, kt_bridge::handle::KIND_EPHEMERAL)
        .unwrap();
    let (finished, completion) = std::sync::mpsc::channel();
    let tracker = rt.tasks.clone();
    let closer = std::thread::spawn(move || {
        runtime::free_runtime(rt);
        finished.send(()).unwrap();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !tracker.is_closed() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let entered_shutdown = tracker.is_closed();
    let waiting = completion.recv_timeout(Duration::from_millis(100)).is_err();
    drop(removed);
    completion.recv_timeout(Duration::from_secs(5)).unwrap();
    closer.join().unwrap();
    assert!(entered_shutdown, "closer did not enter runtime shutdown");
    assert!(
        waiting,
        "runtime free returned before its removed child was reaped"
    );
    assert_reaped(pid);
}
