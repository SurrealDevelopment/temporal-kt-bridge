use kt_bridge::{proto, worker};
use std::time::Duration;
use temporalio_sdk_core::{PollerBehavior, WorkerVersioningStrategy, WorkflowErrorType};

fn options() -> proto::WorkerOptions {
    proto::WorkerOptions {
        namespace: "namespace".into(),
        task_queue: "queue".into(),
        ..Default::default()
    }
}

#[test]
fn worker_options_reach_core_including_presence_and_nexus() {
    let input = proto::WorkerOptions {
        max_cached_workflows: 20,
        identity: "identity".into(),
        build_id: "build".into(),
        no_remote_activities: true,
        no_local_activities: true,
        enable_nexus: true,
        max_concurrent_workflow_tasks: Some(7),
        max_concurrent_activities: Some(8),
        max_concurrent_local_activities: Some(9),
        max_concurrent_nexus_tasks: Some(11),
        workflow_poller_behavior: Some(proto::PollerBehavior {
            strategy: Some(proto::poller_behavior::Strategy::SimpleMaximum(3)),
        }),
        activity_poller_behavior: Some(proto::PollerBehavior {
            strategy: Some(proto::poller_behavior::Strategy::Autoscaling(
                proto::PollerAutoscaling {
                    minimum: 2,
                    maximum: 8,
                    initial: 4,
                },
            )),
        }),
        max_heartbeat_throttle_interval_millis: Some(500),
        default_heartbeat_throttle_interval_millis: Some(250),
        max_activities_per_second: Some(2.5),
        max_task_queue_activities_per_second: Some(3.5),
        nonsticky_to_sticky_poll_ratio: Some(0.5),
        sticky_queue_schedule_to_start_timeout_millis: Some(200),
        graceful_shutdown_period_millis: Some(0),
        nondeterminism_as_workflow_fail_for_types: vec!["Workflow".into()],
        max_eager_activity_reservations_per_workflow_task: Some(0),
        disable_payload_error_limit: true,
        ..options()
    };
    let config = worker::worker_config(&input).unwrap();
    assert_eq!(config.namespace, "namespace");
    assert_eq!(config.task_queue, "queue");
    assert_eq!(config.client_identity_override.as_deref(), Some("identity"));
    assert_eq!(config.max_cached_workflows, 20);
    assert!(config.task_types.enable_workflows && config.task_types.enable_nexus);
    assert!(
        !config.task_types.enable_remote_activities && !config.task_types.enable_local_activities
    );
    assert_eq!(config.versioning_strategy.build_id(), "build");
    assert_eq!(
        config.workflow_task_poller_behavior,
        Some(PollerBehavior::SimpleMaximum(3))
    );
    assert_eq!(
        config.activity_task_poller_behavior,
        Some(PollerBehavior::Autoscaling {
            minimum: 2,
            maximum: 8,
            initial: 4
        })
    );
    assert_eq!(config.nexus_task_poller_behavior, None);
    assert_eq!(
        config.max_heartbeat_throttle_interval,
        Duration::from_millis(500)
    );
    assert_eq!(
        config.default_heartbeat_throttle_interval,
        Duration::from_millis(250)
    );
    assert_eq!(config.max_worker_activities_per_second, Some(2.5));
    assert_eq!(config.max_task_queue_activities_per_second, Some(3.5));
    assert_eq!(config.nonsticky_to_sticky_poll_ratio, 0.5);
    assert_eq!(
        config.sticky_queue_schedule_to_start_timeout,
        Duration::from_millis(200)
    );
    assert_eq!(config.graceful_shutdown_period, Some(Duration::ZERO));
    assert_eq!(config.max_eager_activity_reservations_per_workflow_task, 0);
    assert!(config.disable_payload_error_limit);
    assert!(config.should_fail_workflow("Workflow", &WorkflowErrorType::Nondeterminism));
    assert!(!config.should_fail_workflow("Other", &WorkflowErrorType::Nondeterminism));
    let tuner = config.tuner.as_ref().unwrap();
    assert_eq!(
        tuner.workflow_task_slot_supplier().available_slots(),
        Some(7)
    );
    assert_eq!(
        tuner.activity_task_slot_supplier().available_slots(),
        Some(8)
    );
    assert_eq!(
        tuner.local_activity_slot_supplier().available_slots(),
        Some(9)
    );
    assert_eq!(tuner.nexus_task_slot_supplier().available_slots(), Some(11));
    let global = worker::worker_config(&proto::WorkerOptions {
        nondeterminism_as_workflow_fail: true,
        ..input
    })
    .unwrap();
    assert!(global.should_fail_workflow("Other", &WorkflowErrorType::Nondeterminism));
}

#[test]
fn unset_fields_keep_core_defaults_while_zero_disables_options() {
    let config = worker::worker_config(&options()).unwrap();
    assert_eq!(config.max_eager_activity_reservations_per_workflow_task, 3);
    assert_eq!(
        config.max_heartbeat_throttle_interval,
        Duration::from_secs(60)
    );
    assert_eq!(
        config.default_heartbeat_throttle_interval,
        Duration::from_secs(30)
    );
    assert_eq!(config.graceful_shutdown_period, None);
    assert_eq!(config.workflow_task_poller_behavior, None);
    let zeros = worker::worker_config(&proto::WorkerOptions {
        max_activities_per_second: Some(0.0),
        max_task_queue_activities_per_second: Some(0.0),
        max_heartbeat_throttle_interval_millis: Some(0),
        max_eager_activity_reservations_per_workflow_task: Some(0),
        ..options()
    })
    .unwrap();
    assert_eq!(zeros.max_worker_activities_per_second, None);
    assert_eq!(zeros.max_task_queue_activities_per_second, None);
    assert_eq!(zeros.max_heartbeat_throttle_interval, Duration::ZERO);
    assert_eq!(zeros.max_eager_activity_reservations_per_workflow_task, 0);
}

#[test]
fn deployment_preserves_version_routing_and_rejects_invalid_behavior() {
    for behavior in 0..=2 {
        let mut input = options();
        input.deployment_options = Some(proto::WorkerDeploymentOptions {
            deployment_name: "deployment".into(),
            build_id: "deployment-build".into(),
            use_worker_versioning: true,
            default_versioning_behavior: behavior,
        });
        input.build_id = "ignored-build".into();
        let config = worker::worker_config(&input).unwrap();
        let WorkerVersioningStrategy::WorkerDeploymentBased(deployment) =
            config.versioning_strategy
        else {
            panic!("deployment routing was lost")
        };
        assert_eq!(deployment.version.deployment_name, "deployment");
        assert_eq!(deployment.version.build_id, "deployment-build");
        assert!(deployment.use_worker_versioning);
        assert_eq!(
            deployment.default_versioning_behavior.is_some(),
            behavior != 0
        );
        input
            .deployment_options
            .as_mut()
            .unwrap()
            .use_worker_versioning = false;
        assert_eq!(worker::worker_config(&input).is_ok(), behavior == 0);
    }
    assert!(
        worker::worker_config(&proto::WorkerOptions {
            deployment_options: Some(proto::WorkerDeploymentOptions {
                deployment_name: "deployment".into(),
                build_id: "build".into(),
                default_versioning_behavior: 999,
                ..Default::default()
            }),
            ..options()
        })
        .is_err()
    );
}

#[test]
fn invalid_native_options_are_rejected_before_constructing_core_suppliers() {
    let invalid = [
        proto::WorkerOptions {
            max_concurrent_activities: Some(0),
            ..options()
        },
        proto::WorkerOptions {
            max_cached_workflows: 1,
            max_concurrent_workflow_tasks: Some(1),
            ..options()
        },
        proto::WorkerOptions {
            max_activities_per_second: Some(f64::NAN),
            ..options()
        },
        proto::WorkerOptions {
            max_task_queue_activities_per_second: Some(-1.0),
            ..options()
        },
        proto::WorkerOptions {
            nonsticky_to_sticky_poll_ratio: Some(f32::INFINITY),
            ..options()
        },
        proto::WorkerOptions {
            graceful_shutdown_period_millis: Some(u64::MAX),
            ..options()
        },
        proto::WorkerOptions {
            workflow_poller_behavior: Some(proto::PollerBehavior::default()),
            ..options()
        },
        proto::WorkerOptions {
            activity_poller_behavior: Some(proto::PollerBehavior {
                strategy: Some(proto::poller_behavior::Strategy::SimpleMaximum(0)),
            }),
            ..options()
        },
        proto::WorkerOptions {
            activity_resource_limits: Some(proto::ResourceSlotLimits {
                minimum_slots: 1,
                maximum_slots: 10,
                ramp_throttle_millis: 0,
            }),
            ..options()
        },
        proto::WorkerOptions {
            resource_tuner: Some(proto::ResourceBasedTuner {
                target_memory_usage: f64::NAN,
                ..Default::default()
            }),
            ..options()
        },
        proto::WorkerOptions {
            resource_tuner: Some(proto::ResourceBasedTuner::default()),
            activity_resource_limits: Some(proto::ResourceSlotLimits {
                minimum_slots: 3,
                maximum_slots: 2,
                ramp_throttle_millis: 0,
            }),
            ..options()
        },
        proto::WorkerOptions {
            resource_tuner: Some(proto::ResourceBasedTuner::default()),
            activity_resource_limits: Some(proto::ResourceSlotLimits::default()),
            ..options()
        },
    ];
    for input in invalid {
        assert!(
            worker::worker_config(&input).is_err(),
            "invalid config accepted: {input:?}"
        );
    }
}
