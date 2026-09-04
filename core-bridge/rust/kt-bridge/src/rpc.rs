//! Raw gRPC dispatch.
//!
//! `RawGrpcCaller::call` is `pub(crate)` in temporalio-client, so there is no generic
//! bytes-in/bytes-out entry point: every RPC has to be named and typed. Bypassing the generated
//! clients with a hand-rolled tonic codec would skip Core's retry, metrics and header
//! interceptors, so the dispatch is explicit instead.
//!
//! GENERATED from the `proxier!` blocks in temporalio-client's `grpc.rs`, which is the source of
//! truth for which RPCs exist. Regenerate with `tools/generate_rpc_table.py` rather than editing
//! by hand -- 140-odd arms drifting silently across an SDK-Core upgrade is the failure mode this
//! rewrite exists to remove.
//!
//! One prost decode and one encode per call is accepted: microseconds against a network round
//! trip.

use temporalio_client::Connection;
// The service traits are not imported: methods are called on the returned
// `Box<dyn WorkflowService>` etc., which is the trait object itself.
use temporalio_common::protos::temporal::api::{
    operatorservice::v1::*, testservice::v1::*, workflowservice::v1::*,
};
use tonic::Request;

use crate::error::{KtError, KtResult};

/// Which gRPC service an RPC belongs to. Mirrors the JVM-side enum.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
pub enum Service {
    Workflow = 0,
    Operator = 1,
    Test = 2,
}

impl Service {
    pub fn from_u32(value: u32) -> KtResult<Self> {
        Ok(match value {
            0 => Service::Workflow,
            1 => Service::Operator,
            2 => Service::Test,
            other => return Err(KtError::InvalidArgument(format!("unknown service {other}"))),
        })
    }
}

/// The outcome of a call.
///
/// A gRPC error is a normal outcome, not a bridge failure: the status code travels back verbatim
/// so the JVM can raise the exception the server intended rather than a generic one.
pub struct RpcOutcome {
    pub payload: Vec<u8>,
    pub status_code: i32,
    pub message: String,
}

macro_rules! dispatch {
    ($client:expr, $rpc:expr, $bytes:expr, { $($name:literal => $method:ident($req:ty)),+ $(,)? }) => {
        match $rpc {
            $(
                $name => {
                    let decoded: $req = prost::Message::decode($bytes)
                        .map_err(|e| KtError::InvalidArgument(format!("{} request: {e}", $name)))?;
                    match $client.$method(Request::new(decoded)).await {
                        Ok(response) => Ok(RpcOutcome {
                            payload: prost::Message::encode_to_vec(&response.into_inner()),
                            status_code: 0,
                            message: String::new(),
                        }),
                        Err(status) => Ok(RpcOutcome {
                            payload: Vec::new(),
                            status_code: status.code() as i32,
                            message: status.message().to_string(),
                        }),
                    }
                }
            )+
            other => Err(KtError::InvalidArgument(format!("unknown rpc {other}"))),
        }
    };
}

/// 123 RPCs on WorkflowService.
async fn call_workflow_service(
    connection: &Connection,
    rpc: &str,
    bytes: &[u8],
) -> KtResult<RpcOutcome> {
    let mut client = connection.workflow_service();
    dispatch!(client, rpc, bytes, {
        "RegisterNamespace" => register_namespace(RegisterNamespaceRequest),
        "DescribeNamespace" => describe_namespace(DescribeNamespaceRequest),
        "ListNamespaces" => list_namespaces(ListNamespacesRequest),
        "UpdateNamespace" => update_namespace(UpdateNamespaceRequest),
        "DeprecateNamespace" => deprecate_namespace(DeprecateNamespaceRequest),
        "StartWorkflowExecution" => start_workflow_execution(StartWorkflowExecutionRequest),
        "GetWorkflowExecutionHistory" => get_workflow_execution_history(GetWorkflowExecutionHistoryRequest),
        "GetWorkflowExecutionHistoryReverse" => get_workflow_execution_history_reverse(GetWorkflowExecutionHistoryReverseRequest),
        "PollWorkflowTaskQueue" => poll_workflow_task_queue(PollWorkflowTaskQueueRequest),
        "RespondWorkflowTaskCompleted" => respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest),
        "RespondWorkflowTaskFailed" => respond_workflow_task_failed(RespondWorkflowTaskFailedRequest),
        "PollActivityTaskQueue" => poll_activity_task_queue(PollActivityTaskQueueRequest),
        "RecordActivityTaskHeartbeat" => record_activity_task_heartbeat(RecordActivityTaskHeartbeatRequest),
        "RecordActivityTaskHeartbeatById" => record_activity_task_heartbeat_by_id(RecordActivityTaskHeartbeatByIdRequest),
        "RespondActivityTaskCompleted" => respond_activity_task_completed(RespondActivityTaskCompletedRequest),
        "RespondActivityTaskCompletedById" => respond_activity_task_completed_by_id(RespondActivityTaskCompletedByIdRequest),
        "RespondActivityTaskFailed" => respond_activity_task_failed(RespondActivityTaskFailedRequest),
        "RespondActivityTaskFailedById" => respond_activity_task_failed_by_id(RespondActivityTaskFailedByIdRequest),
        "RespondActivityTaskCanceled" => respond_activity_task_canceled(RespondActivityTaskCanceledRequest),
        "RespondActivityTaskCanceledById" => respond_activity_task_canceled_by_id(RespondActivityTaskCanceledByIdRequest),
        "RequestCancelWorkflowExecution" => request_cancel_workflow_execution(RequestCancelWorkflowExecutionRequest),
        "SignalWorkflowExecution" => signal_workflow_execution(SignalWorkflowExecutionRequest),
        "SignalWithStartWorkflowExecution" => signal_with_start_workflow_execution(SignalWithStartWorkflowExecutionRequest),
        "ResetWorkflowExecution" => reset_workflow_execution(ResetWorkflowExecutionRequest),
        "TerminateWorkflowExecution" => terminate_workflow_execution(TerminateWorkflowExecutionRequest),
        "DeleteWorkflowExecution" => delete_workflow_execution(DeleteWorkflowExecutionRequest),
        "ListOpenWorkflowExecutions" => list_open_workflow_executions(ListOpenWorkflowExecutionsRequest),
        "ListClosedWorkflowExecutions" => list_closed_workflow_executions(ListClosedWorkflowExecutionsRequest),
        "ListWorkflowExecutions" => list_workflow_executions(ListWorkflowExecutionsRequest),
        "ListArchivedWorkflowExecutions" => list_archived_workflow_executions(ListArchivedWorkflowExecutionsRequest),
        "ScanWorkflowExecutions" => scan_workflow_executions(ScanWorkflowExecutionsRequest),
        "CountWorkflowExecutions" => count_workflow_executions(CountWorkflowExecutionsRequest),
        "CreateWorkflowRule" => create_workflow_rule(CreateWorkflowRuleRequest),
        "DescribeWorkflowRule" => describe_workflow_rule(DescribeWorkflowRuleRequest),
        "DeleteWorkflowRule" => delete_workflow_rule(DeleteWorkflowRuleRequest),
        "ListWorkflowRules" => list_workflow_rules(ListWorkflowRulesRequest),
        "TriggerWorkflowRule" => trigger_workflow_rule(TriggerWorkflowRuleRequest),
        "GetSearchAttributes" => get_search_attributes(GetSearchAttributesRequest),
        "RespondQueryTaskCompleted" => respond_query_task_completed(RespondQueryTaskCompletedRequest),
        "ResetStickyTaskQueue" => reset_sticky_task_queue(ResetStickyTaskQueueRequest),
        "QueryWorkflow" => query_workflow(QueryWorkflowRequest),
        "DescribeWorkflowExecution" => describe_workflow_execution(DescribeWorkflowExecutionRequest),
        "DescribeTaskQueue" => describe_task_queue(DescribeTaskQueueRequest),
        "GetClusterInfo" => get_cluster_info(GetClusterInfoRequest),
        "GetSystemInfo" => get_system_info(GetSystemInfoRequest),
        "ListTaskQueuePartitions" => list_task_queue_partitions(ListTaskQueuePartitionsRequest),
        "CreateSchedule" => create_schedule(CreateScheduleRequest),
        "DescribeSchedule" => describe_schedule(DescribeScheduleRequest),
        "UpdateSchedule" => update_schedule(UpdateScheduleRequest),
        "PatchSchedule" => patch_schedule(PatchScheduleRequest),
        "ListScheduleMatchingTimes" => list_schedule_matching_times(ListScheduleMatchingTimesRequest),
        "DeleteSchedule" => delete_schedule(DeleteScheduleRequest),
        "ListSchedules" => list_schedules(ListSchedulesRequest),
        "CountSchedules" => count_schedules(CountSchedulesRequest),
        "UpdateWorkerBuildIdCompatibility" => update_worker_build_id_compatibility(UpdateWorkerBuildIdCompatibilityRequest),
        "GetWorkerBuildIdCompatibility" => get_worker_build_id_compatibility(GetWorkerBuildIdCompatibilityRequest),
        "GetWorkerTaskReachability" => get_worker_task_reachability(GetWorkerTaskReachabilityRequest),
        "UpdateWorkflowExecution" => update_workflow_execution(UpdateWorkflowExecutionRequest),
        "PollWorkflowExecutionUpdate" => poll_workflow_execution_update(PollWorkflowExecutionUpdateRequest),
        "StartBatchOperation" => start_batch_operation(StartBatchOperationRequest),
        "StopBatchOperation" => stop_batch_operation(StopBatchOperationRequest),
        "DescribeBatchOperation" => describe_batch_operation(DescribeBatchOperationRequest),
        "DescribeDeployment" => describe_deployment(DescribeDeploymentRequest),
        "ListBatchOperations" => list_batch_operations(ListBatchOperationsRequest),
        "ListDeployments" => list_deployments(ListDeploymentsRequest),
        "ExecuteMultiOperation" => execute_multi_operation(ExecuteMultiOperationRequest),
        "GetCurrentDeployment" => get_current_deployment(GetCurrentDeploymentRequest),
        "GetDeploymentReachability" => get_deployment_reachability(GetDeploymentReachabilityRequest),
        "GetWorkerVersioningRules" => get_worker_versioning_rules(GetWorkerVersioningRulesRequest),
        "UpdateWorkerVersioningRules" => update_worker_versioning_rules(UpdateWorkerVersioningRulesRequest),
        "PollNexusTaskQueue" => poll_nexus_task_queue(PollNexusTaskQueueRequest),
        "RespondNexusTaskCompleted" => respond_nexus_task_completed(RespondNexusTaskCompletedRequest),
        "RespondNexusTaskFailed" => respond_nexus_task_failed(RespondNexusTaskFailedRequest),
        "SetCurrentDeployment" => set_current_deployment(SetCurrentDeploymentRequest),
        "ShutdownWorker" => shutdown_worker(ShutdownWorkerRequest),
        "UpdateActivityOptions" => update_activity_options(UpdateActivityOptionsRequest),
        "PauseActivity" => pause_activity(PauseActivityRequest),
        "UnpauseActivity" => unpause_activity(UnpauseActivityRequest),
        "UpdateWorkflowExecutionOptions" => update_workflow_execution_options(UpdateWorkflowExecutionOptionsRequest),
        "ResetActivity" => reset_activity(ResetActivityRequest),
        "DeleteWorkerDeployment" => delete_worker_deployment(DeleteWorkerDeploymentRequest),
        "DeleteWorkerDeploymentVersion" => delete_worker_deployment_version(DeleteWorkerDeploymentVersionRequest),
        "DescribeWorkerDeployment" => describe_worker_deployment(DescribeWorkerDeploymentRequest),
        "DescribeWorkerDeploymentVersion" => describe_worker_deployment_version(DescribeWorkerDeploymentVersionRequest),
        "ListWorkerDeployments" => list_worker_deployments(ListWorkerDeploymentsRequest),
        "SetWorkerDeploymentCurrentVersion" => set_worker_deployment_current_version(SetWorkerDeploymentCurrentVersionRequest),
        "SetWorkerDeploymentRampingVersion" => set_worker_deployment_ramping_version(SetWorkerDeploymentRampingVersionRequest),
        "UpdateWorkerDeploymentVersionMetadata" => update_worker_deployment_version_metadata(UpdateWorkerDeploymentVersionMetadataRequest),
        "ListWorkers" => list_workers(ListWorkersRequest),
        "CountWorkers" => count_workers(CountWorkersRequest),
        "RecordWorkerHeartbeat" => record_worker_heartbeat(RecordWorkerHeartbeatRequest),
        "UpdateTaskQueueConfig" => update_task_queue_config(UpdateTaskQueueConfigRequest),
        "FetchWorkerConfig" => fetch_worker_config(FetchWorkerConfigRequest),
        "UpdateWorkerConfig" => update_worker_config(UpdateWorkerConfigRequest),
        "DescribeWorker" => describe_worker(DescribeWorkerRequest),
        "SetWorkerDeploymentManager" => set_worker_deployment_manager(SetWorkerDeploymentManagerRequest),
        "PauseWorkflowExecution" => pause_workflow_execution(PauseWorkflowExecutionRequest),
        "UnpauseWorkflowExecution" => unpause_workflow_execution(UnpauseWorkflowExecutionRequest),
        "StartActivityExecution" => start_activity_execution(StartActivityExecutionRequest),
        "DescribeActivityExecution" => describe_activity_execution(DescribeActivityExecutionRequest),
        "PollActivityExecution" => poll_activity_execution(PollActivityExecutionRequest),
        "ListActivityExecutions" => list_activity_executions(ListActivityExecutionsRequest),
        "CountActivityExecutions" => count_activity_executions(CountActivityExecutionsRequest),
        "RequestCancelActivityExecution" => request_cancel_activity_execution(RequestCancelActivityExecutionRequest),
        "TerminateActivityExecution" => terminate_activity_execution(TerminateActivityExecutionRequest),
        "DeleteActivityExecution" => delete_activity_execution(DeleteActivityExecutionRequest),
        "PauseActivityExecution" => pause_activity_execution(PauseActivityExecutionRequest),
        "UnpauseActivityExecution" => unpause_activity_execution(UnpauseActivityExecutionRequest),
        "ResetActivityExecution" => reset_activity_execution(ResetActivityExecutionRequest),
        "UpdateActivityExecutionOptions" => update_activity_execution_options(UpdateActivityExecutionOptionsRequest),
        "CountNexusOperationExecutions" => count_nexus_operation_executions(CountNexusOperationExecutionsRequest),
        "CreateWorkerDeployment" => create_worker_deployment(CreateWorkerDeploymentRequest),
        "CreateWorkerDeploymentVersion" => create_worker_deployment_version(CreateWorkerDeploymentVersionRequest),
        "DeleteNexusOperationExecution" => delete_nexus_operation_execution(DeleteNexusOperationExecutionRequest),
        "DescribeNexusOperationExecution" => describe_nexus_operation_execution(DescribeNexusOperationExecutionRequest),
        "ListNexusOperationExecutions" => list_nexus_operation_executions(ListNexusOperationExecutionsRequest),
        "PollNexusOperationExecution" => poll_nexus_operation_execution(PollNexusOperationExecutionRequest),
        "PollWorkflowExecutionTimeSkipping" => poll_workflow_execution_time_skipping(PollWorkflowExecutionTimeSkippingRequest),
        "RequestCancelNexusOperationExecution" => request_cancel_nexus_operation_execution(RequestCancelNexusOperationExecutionRequest),
        "StartNexusOperationExecution" => start_nexus_operation_execution(StartNexusOperationExecutionRequest),
        "TerminateNexusOperationExecution" => terminate_nexus_operation_execution(TerminateNexusOperationExecutionRequest),
        "UpdateWorkerDeploymentVersionComputeConfig" => update_worker_deployment_version_compute_config(UpdateWorkerDeploymentVersionComputeConfigRequest),
        "ValidateWorkerDeploymentVersionComputeConfig" => validate_worker_deployment_version_compute_config(ValidateWorkerDeploymentVersionComputeConfigRequest)
    })
}

/// 11 RPCs on OperatorService.
async fn call_operator_service(
    connection: &Connection,
    rpc: &str,
    bytes: &[u8],
) -> KtResult<RpcOutcome> {
    let mut client = connection.operator_service();
    dispatch!(client, rpc, bytes, {
        "AddSearchAttributes" => add_search_attributes(AddSearchAttributesRequest),
        "RemoveSearchAttributes" => remove_search_attributes(RemoveSearchAttributesRequest),
        "ListSearchAttributes" => list_search_attributes(ListSearchAttributesRequest),
        "AddOrUpdateRemoteCluster" => add_or_update_remote_cluster(AddOrUpdateRemoteClusterRequest),
        "RemoveRemoteCluster" => remove_remote_cluster(RemoveRemoteClusterRequest),
        "ListClusters" => list_clusters(ListClustersRequest),
        "GetNexusEndpoint" => get_nexus_endpoint(GetNexusEndpointRequest),
        "CreateNexusEndpoint" => create_nexus_endpoint(CreateNexusEndpointRequest),
        "UpdateNexusEndpoint" => update_nexus_endpoint(UpdateNexusEndpointRequest),
        "DeleteNexusEndpoint" => delete_nexus_endpoint(DeleteNexusEndpointRequest),
        "ListNexusEndpoints" => list_nexus_endpoints(ListNexusEndpointsRequest)
    })
}

/// 6 RPCs on TestService.
async fn call_test_service(
    connection: &Connection,
    rpc: &str,
    bytes: &[u8],
) -> KtResult<RpcOutcome> {
    let mut client = connection.test_service();
    if let Some(outcome) = call_test_service_unit(&mut client, rpc).await? {
        return Ok(outcome);
    }
    dispatch!(client, rpc, bytes, {
        "LockTimeSkipping" => lock_time_skipping(LockTimeSkippingRequest),
        "UnlockTimeSkipping" => unlock_time_skipping(UnlockTimeSkippingRequest),
        "Sleep" => sleep(SleepRequest),
        "SleepUntil" => sleep_until(SleepUntilRequest),
        "UnlockTimeSkippingWithSleep" => unlock_time_skipping_with_sleep(SleepRequest)
    })
}

/// Unit-request RPCs on TestService: nothing to decode, so they bypass `dispatch!`.
async fn call_test_service_unit(
    client: &mut Box<dyn temporalio_client::grpc::TestService>,
    rpc: &str,
) -> KtResult<Option<RpcOutcome>> {
    let result = match rpc {
        "GetCurrentTime" => Some(client.get_current_time(Request::new(())).await),
        _ => None,
    };
    Ok(result.map(|r| match r {
        Ok(response) => RpcOutcome {
            payload: prost::Message::encode_to_vec(&response.into_inner()),
            status_code: 0,
            message: String::new(),
        },
        Err(status) => RpcOutcome {
            payload: Vec::new(),
            status_code: status.code() as i32,
            message: status.message().to_string(),
        },
    }))
}

/// Dispatches one RPC by service and name.
pub async fn call(
    connection: &Connection,
    service: Service,
    rpc: &str,
    bytes: &[u8],
) -> KtResult<RpcOutcome> {
    match service {
        Service::Workflow => call_workflow_service(connection, rpc, bytes).await,
        Service::Operator => call_operator_service(connection, rpc, bytes).await,
        Service::Test => call_test_service(connection, rpc, bytes).await,
    }
}
