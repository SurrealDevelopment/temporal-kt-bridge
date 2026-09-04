//! Raw gRPC dispatch against a real server.
//!
//! The table is generated, so a mistyped arm is a compile error. What a test can add is that the
//! whole path works: the name maps to the right RPC, the response decodes, and a gRPC error keeps
//! the server's own status code instead of collapsing into a generic bridge failure.
//!
//! Set TEMPORAL_TEST_ADDRESS to run; skipped loudly otherwise.

use kt_bridge::{client, proto, rpc};
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    DescribeNamespaceRequest, DescribeNamespaceResponse, StartWorkflowExecutionRequest,
};

#[test]
fn rpcs_round_trip_and_grpc_errors_keep_the_servers_status_code() {
    let Some(address) = std::env::var("TEMPORAL_TEST_ADDRESS").ok().filter(|s| !s.is_empty())
    else {
        eprintln!("skipping: set TEMPORAL_TEST_ADDRESS to run this against a dev server");
        return;
    };

    let rt = runtime_for_test();
    let options = client::connection_options(&proto::ClientOptions {
        target_url: format!("http://{address}"),
        namespace: "default".into(),
        ..Default::default()
    })
    .expect("options");

    // Driven directly on Core's runtime: this exercises rpc::call, not the completion queue,
    // which the other integration tests already cover.
    rt.block_on(async {
        let connection = client::connect(options, "default".into())
            .await
            .expect("connect");

        // A successful call must decode as the response type the name implies.
        let request = prost::Message::encode_to_vec(&DescribeNamespaceRequest {
            namespace: "default".into(),
            ..Default::default()
        });
        let ok = rpc::call(&connection.connection, rpc::Service::Workflow, "DescribeNamespace", &request)
            .await
            .expect("dispatch");
        assert_eq!(ok.status_code, 0, "DescribeNamespace failed: {}", ok.message);
        let decoded: DescribeNamespaceResponse =
            prost::Message::decode(ok.payload.as_slice()).expect("response must decode");
        assert_eq!(decoded.namespace_info.expect("namespace_info").name, "default");

        // A server rejection keeps its gRPC code, so the JVM can raise what the server intended.
        let empty = prost::Message::encode_to_vec(&StartWorkflowExecutionRequest::default());
        let rejected = rpc::call(&connection.connection, rpc::Service::Workflow, "StartWorkflowExecution", &empty)
            .await
            .expect("dispatch");
        assert!(rejected.status_code > 0, "expected a gRPC status, got {}", rejected.status_code);
        eprintln!("empty StartWorkflowExecution -> gRPC {}", rejected.status_code);

        // An unknown name is rejected rather than silently doing nothing.
        assert!(
            rpc::call(&connection.connection, rpc::Service::Workflow, "NoSuchRpc", &[])
                .await
                .is_err()
        );
    });
}

fn runtime_for_test() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio")
}
