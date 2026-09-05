use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use kt_bridge::{client, proto, rpc};
use prost::Message;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_common::protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryRequest;

#[tokio::test]
async fn transport_preserves_identity_retries_deadlines_and_grpc_details() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = attempts.clone();
    let mut options = client::connection_options(&proto::ClientOptions {
        target_url: "http://localhost:7233".into(),
        client_name: "temporal-kotlin-test".into(),
        client_version: "9.8.7".into(),
        api_key: "test-key".into(),
        ..Default::default()
    })
    .unwrap();
    options.dns_load_balancing = None;
    options.service_override = Some(CallbackBasedGrpcService {
        callback: Arc::new(move |request| {
            let attempts = observed_attempts.clone();
            Box::pin(async move {
                assert_eq!(request.headers["client-name"], "temporal-kotlin-test");
                assert_eq!(request.headers["client-version"], "9.8.7");
                assert_eq!(request.headers["authorization"], "Bearer test-key");
                match request.rpc.as_str() {
                    "DescribeNamespace" => {
                        assert_eq!(request.headers["grpc-timeout"], "90000000u");
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Err(tonic::Status::unavailable("retry this request"));
                        }
                    }
                    "GetWorkflowExecutionHistory" => {
                        assert_eq!(request.headers["grpc-timeout"], "70000000u");
                    }
                    "StartWorkflowExecution" => {
                        return Err(tonic::Status::with_details(
                            tonic::Code::AlreadyExists,
                            "already started",
                            vec![1, 2, 3].into(),
                        ));
                    }
                    "GetClusterInfo" => tokio::time::sleep(Duration::from_secs(60)).await,
                    "GetSystemInfo" => {}
                    other => panic!("unexpected RPC {other}"),
                }
                Ok(GrpcSuccessResponse {
                    headers: Default::default(),
                    proto: Vec::new(),
                })
            })
        }),
    });
    let client = temporalio_client::Connection::connect(options)
        .await
        .unwrap();
    let outcome = rpc::call(
        &client,
        rpc::Service::Workflow,
        "DescribeNamespace",
        &[],
        Some(Duration::from_secs(90)),
    )
    .await
    .unwrap();
    assert_eq!(outcome.status_code, 0);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let history = GetWorkflowExecutionHistoryRequest {
        wait_new_event: true,
        ..Default::default()
    };
    let outcome = rpc::call(
        &client,
        rpc::Service::Workflow,
        "GetWorkflowExecutionHistory",
        &history.encode_to_vec(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome.status_code, 0);

    let outcome = rpc::call(
        &client,
        rpc::Service::Workflow,
        "StartWorkflowExecution",
        &[],
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome.status_code, tonic::Code::AlreadyExists as i32);
    let failure = proto::RpcFailure::decode(outcome.payload.as_slice()).unwrap();
    assert_eq!(failure.message, "already started");
    assert_eq!(failure.details, vec![1, 2, 3]);

    let outcome = rpc::call(
        &client,
        rpc::Service::Workflow,
        "GetClusterInfo",
        &[],
        Some(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    assert_eq!(outcome.status_code, tonic::Code::DeadlineExceeded as i32);
}
