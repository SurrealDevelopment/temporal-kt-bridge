//! Raw gRPC dispatch against a real server.
//!
//! The dispatch table is generated from temporalio-client's `proxier!` blocks, so the risk is not
//! that an individual arm is mistyped -- the compiler catches that -- but that the whole path is
//! wrong: the wrong name mapping, a response that does not decode, or a gRPC error surfacing as a
//! bridge failure instead of the server's own status code.
//!
//! Set TEMPORAL_TEST_ADDRESS to run; skipped loudly otherwise.

use std::time::Duration;

use kt_bridge::abi::{KT_OK, KtCompletion};
use kt_bridge::{client, proto, queue, rpc, runtime};
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    DescribeNamespaceResponse, StartWorkflowExecutionRequest,
};

fn address() -> Option<String> {
    std::env::var("TEMPORAL_TEST_ADDRESS").ok().filter(|s| !s.is_empty())
}

fn wait_for(poller: &queue::PollerEntry, want: u64) -> Option<KtCompletion> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        let mut out = vec![
            KtCompletion { req_id: 0, kind: 0, status: 0, payload: 0, payload_len: 0, aux0: 0, aux1: 0 };
            8
        ];
        let n = unsafe { poller.poll(out.as_mut_ptr(), 8, 250) }.expect("poll");
        for record in &out[..n as usize] {
            if record.req_id == want {
                return Some(*record);
            }
        }
    }
    None
}

fn payload(record: &KtCompletion) -> &[u8] {
    if record.payload_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(record.payload as *const u8, record.payload_len as usize) }
    }
}

#[test]
fn rpcs_round_trip_and_grpc_errors_keep_the_servers_status_code() {
    let Some(address) = address() else {
        eprintln!("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server");
        return;
    };

    let rt = runtime::new_runtime(proto::RuntimeOptions::default()).expect("runtime");
    let poller = rt.queue.poller();

    let options = client::connection_options(&proto::ClientOptions {
        target_url: format!("http://{address}"),
        namespace: "default".into(),
        ..Default::default()
    })
    .expect("options");

    let connection = {
        let _entered = rt.core.tokio_handle().enter();
        futures_util::future::block_on_or_spawn(&rt, options)
    };

    // 1. A successful RPC decodes into the right response type.
    let conn = connection.clone();
    runtime::spawn_request(&rt, 1, async move {
        let request = prost::Message::encode_to_vec(
            &temporalio_common::protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest {
                namespace: "default".into(),
                ..Default::default()
            },
        );
        match rpc::call(&conn, rpc::Service::Workflow, "DescribeNamespace", &request).await {
            Ok(o) if o.status_code == 0 => queue::Pending::ack(1).payload(o.payload),
            Ok(o) => queue::Pending::error(1, o.status_code, o.message),
            Err(e) => queue::Pending::error(1, e.code(), e.to_string()),
        }
    });
    let described = wait_for(&poller, 1).expect("DescribeNamespace produced no completion");
    assert_eq!(described.status, KT_OK, "DescribeNamespace failed");
    let decoded: DescribeNamespaceResponse =
        prost::Message::decode(payload(&described)).expect("response must decode");
    assert_eq!(
        decoded.namespace_info.expect("namespace_info").name,
        "default",
        "the response must be the one the RPC name asked for"
    );

    // 2. A gRPC error keeps the server's status code rather than becoming a bridge failure.
    let conn = connection.clone();
    runtime::spawn_request(&rt, 2, async move {
        let request = prost::Message::encode_to_vec(&StartWorkflowExecutionRequest::default());
        match rpc::call(&conn, rpc::Service::Workflow, "StartWorkflowExecution", &request).await {
            Ok(o) if o.status_code == 0 => queue::Pending::ack(2).payload(o.payload),
            Ok(o) => queue::Pending::error(2, o.status_code, o.message),
            Err(e) => queue::Pending::error(2, e.code(), e.to_string()),
        }
    });
    let rejected = wait_for(&poller, 2).expect("StartWorkflowExecution produced no completion");
    assert!(
        rejected.status > 0,
        "a server rejection must surface as its positive gRPC code, got {}",
        rejected.status
    );
    eprintln!("gRPC status for an empty StartWorkflowExecution: {}", rejected.status);

    // 3. An unknown RPC name is rejected rather than silently doing nothing.
    let conn = connection.clone();
    runtime::spawn_request(&rt, 3, async move {
        match rpc::call(&conn, rpc::Service::Workflow, "NoSuchRpc", &[]).await {
            Ok(_) => queue::Pending::ack(3),
            Err(e) => queue::Pending::error(3, e.code(), e.to_string()),
        }
    });
    let unknown = wait_for(&poller, 3).expect("unknown rpc produced no completion");
    assert!(unknown.status < 0, "an unknown rpc must be a bridge error");

    runtime::free_runtime(rt);
}
