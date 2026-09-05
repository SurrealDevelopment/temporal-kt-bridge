#!/usr/bin/env python3
"""Regenerate src/rpc.rs from temporalio-client's `proxier!` blocks.

Those blocks are the source of truth for which RPCs exist, so the dispatch table is derived from
them rather than hand-maintained: 140-odd match arms drifting silently across an SDK-Core upgrade
is exactly the failure mode this rewrite exists to remove.

Usage:
    python3 tools/generate_rpc_table.py ~/.cargo/registry/src/*/temporalio-client-<ver>/src/grpc.rs
"""
import io
import re
import sys

# Services the SDK actually uses.
#
# CloudService (76 RPCs) is omitted: temporal-kt does not call it. HealthService is omitted too --
# its messages come from the standard gRPC health proto rather than Temporal's, so they are not in
# the imported modules, and its only non-streaming RPC is a liveness check the SDK never makes.
SERVICES = [
    ("WorkflowService", "Workflow", "workflow_service"),
    ("OperatorService", "Operator", "operator_service"),
    ("TestService", "Test", "test_service"),
]


def pascal(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))


def extract(source: str, service: str):
    """Returns [(snake, request_type, response_type)] for one proxier! block."""
    start = source.index(f"proxier! {{\n    {service};")
    end = source.index("\n}\n", start)
    block = source[start:end]
    # Multi-line entries (WorkflowService) and single-line ones (the rest).
    multi = re.findall(
        r"^\s*\(\s*([a-z_0-9]+),\s*\n\s*([A-Za-z0-9:<>]+),\s*\n\s*([A-Za-z0-9:<>]+)", block, re.M
    )
    single = re.findall(
        r"^\s*\(\s*([a-z_0-9]+),\s*([A-Za-z0-9:<>()]+),\s*([A-Za-z0-9:<>()]+)\s*\)", block, re.M
    )
    seen, out = set(), []
    for name, req, resp in multi + single:
        if name in seen:
            continue
        seen.add(name)
        out.append((name, req, resp))
    return out


def usable(name, req, resp):
    """Whether an RPC can be dispatched as bytes-in/bytes-out."""
    if resp.startswith("tonic::codec::Streaming") or "Streaming" in resp:
        return False, "streaming response cannot be returned as one encoded message"
    if req == "()":
        # Not skipped: a unit request just means there is nothing to decode. The SDK's
        # time-skipping test server needs GetCurrentTime, so it gets its own arm.
        return True, None
    return True, None


def main() -> int:
    grpc_rs = sys.argv[1]
    source = io.open(grpc_rs, encoding="utf-8").read()

    blocks, skipped, total = [], [], 0
    for service, _key, accessor in SERVICES:
        rpcs = extract(source, service)
        arms, unit_arms = [], []
        for name, req, resp in rpcs:
            ok, why = usable(name, req, resp)
            if not ok:
                skipped.append(f"{service}.{pascal(name)}: {why}")
                continue
            if req == "()":
                unit_arms.append(f'        "{pascal(name)}" => {name}()')
            else:
                arms.append(f'        "{pascal(name)}" => {name}({req})')
        total += len(arms) + len(unit_arms)
        blocks.append((service, accessor, len(arms) + len(unit_arms), ",\n".join(arms), ",\n".join(unit_arms)))

    parts = [HEADER]
    for service, accessor, count, arms, unit_arms in blocks:
        # RPCs taking a unit request are dispatched separately: there is nothing to decode, so
        # they cannot share the macro that decodes a protobuf first.
        unit_block = (
            f"    match rpc {{\n{unit_arms},\n        _ => {{}}\n    }}\n" if unit_arms else ""
        )
        unit_impl = ""
        if unit_arms:
            unit_impl = (
                "    if let Some(outcome) = "
                f"call_{accessor}_unit(&mut client, rpc, timeout).await? {{\n        return Ok(outcome);\n    }}\n"
            )
        parts.append(
            f"\n/// {count} RPCs on {service}.\n"
            f"async fn call_{accessor}(\n"
            f"    connection: &Connection,\n"
            f"    rpc: &str,\n"
            f"    bytes: &[u8],\n"
            f"    timeout: Option<std::time::Duration>,\n"
            f") -> KtResult<RpcOutcome> {{\n"
            f"    let mut client = connection.clone();\n"
            f"{unit_impl}"
            f"    dispatch!(client, rpc, bytes, timeout, {{\n{arms}\n    }})\n}}\n"
        )
        if unit_arms:
            names = [a.split('"')[1] for a in unit_arms.split("\n")]
            methods = [a.split("=> ")[1].split("(")[0] for a in unit_arms.split("\n")]
            cases = "\n".join(
                f'        "{n}" => Some(with_deadline(timeout, client.{m}(request)).await),'
                for n, m in zip(names, methods)
            )
            parts.append(
                f"\n/// Unit-request RPCs on {service}: nothing to decode, so they bypass `dispatch!`.\n"
                f"async fn call_{accessor}_unit(\n"
                f"    client: &mut Connection,\n"
                f"    rpc: &str,\n"
                f"    timeout: Option<std::time::Duration>,\n"
                f") -> KtResult<Option<RpcOutcome>> {{\n"
                "    let mut request = Request::new(());\n"
                "    if let Some(timeout) = timeout { request.set_timeout(timeout); }\n"
                f"    let result = match rpc {{\n{cases}\n        _ => None,\n    }};\n"
                f"    Ok(result.map(|r| match r {{\n"
                f"        Ok(response) => RpcOutcome {{\n"
                f"            payload: prost::Message::encode_to_vec(&response.into_inner()),\n"
                f"            status_code: 0,\n"
                f"            message: String::new(),\n"
                f"        }},\n"
                f"        Err(status) => RpcOutcome {{\n"
                f"            payload: prost::Message::encode_to_vec(&crate::proto::RpcFailure {{\n"
                f"                message: status_message(&status),\n"
                f"                details: status.details().to_vec(),\n"
                f"            }}),\n"
                f"            status_code: grpc_status_code(&status),\n"
                f"            message: status_message(&status),\n"
                f"        }},\n"
                f"    }}))\n}}\n"
            )
    parts.append(FOOTER)
    io.open("src/rpc.rs", "w", encoding="utf-8").write("".join(parts))

    print(f"generated src/rpc.rs: {total} RPCs across {len(blocks)} services")
    for note in skipped:
        print(f"  skipped {note}")
    return 0


HEADER = '''//! Raw gRPC dispatch.
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
use temporalio_client::grpc::{OperatorService, TestService, WorkflowService};
use temporalio_common::protos::temporal::api::{
    operatorservice::v1::*, testservice::v1::*, workflowservice::v1::*,
};
use tonic::Request;

use crate::error::{KtError, KtResult, grpc_status_code};

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

/// Bounds a call with a client-side deadline, reporting DEADLINE_EXCEEDED when it elapses.
///
/// This outer timeout covers every retry. The request also carries a timeout so Core's default
/// 30-second per-attempt deadline does not shorten an explicitly longer call.
async fn with_deadline<T>(
    timeout: Option<std::time::Duration>,
    call: impl std::future::Future<Output = Result<T, tonic::Status>>,
) -> Result<T, tonic::Status> {
    match timeout {
        None => call.await,
        Some(t) => match tokio::time::timeout(t, call).await {
            Ok(result) => result,
            Err(_) => Err(tonic::Status::deadline_exceeded(format!(
                "client deadline of {t:?} elapsed"
            ))),
        },
    }
}

/// The status message, falling back to the transport error chain when gRPC gave none.
///
/// tonic reports an h2/hyper failure (connection closed, stream reset) as UNKNOWN with an empty
/// message and the real cause only in `source()`. An empty "GetWorkflowExecutionHistory failed:"
/// tells the caller nothing; the chain says what actually happened to the connection.
fn status_message(status: &tonic::Status) -> String {
    if !status.message().is_empty() {
        return status.message().to_string();
    }
    let mut parts = Vec::new();
    let mut cur: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(err) = cur {
        parts.push(err.to_string());
        cur = err.source();
    }
    if parts.is_empty() {
        format!("{:?} with no message", status.code())
    } else {
        format!("{:?}: {}", status.code(), parts.join(": "))
    }
}

macro_rules! dispatch {
    ($client:expr, $rpc:expr, $bytes:expr, $timeout:expr, { $($name:literal => $method:ident($req:ty)),+ $(,)? }) => {
        match $rpc {
            $(
                $name => {
                    let decoded: $req = prost::Message::decode($bytes)
                        .map_err(|e| KtError::InvalidArgument(format!("{} request: {e}", $name)))?;
                    let mut request = Request::new(decoded);
                    if let Some(timeout) = $timeout {
                        request.set_timeout(timeout);
                    }
                    match with_deadline($timeout, $client.$method(request)).await {
                        Ok(response) => Ok(RpcOutcome {
                            payload: prost::Message::encode_to_vec(&response.into_inner()),
                            status_code: 0,
                            message: String::new(),
                        }),
                        Err(status) => Ok(RpcOutcome {
                            payload: prost::Message::encode_to_vec(&crate::proto::RpcFailure {
                                message: status_message(&status),
                                details: status.details().to_vec(),
                            }),
                            status_code: grpc_status_code(&status),
                            message: status_message(&status),
                        }),
                    }
                }
            )+
            other => Err(KtError::InvalidArgument(format!("unknown rpc {other}"))),
        }
    };
}
'''

FOOTER = '''
/// Dispatches one RPC by service and name.
pub async fn call(
    connection: &Connection,
    service: Service,
    rpc: &str,
    bytes: &[u8],
    timeout: Option<std::time::Duration>,
) -> KtResult<RpcOutcome> {
    match service {
        Service::Workflow => call_workflow_service(connection, rpc, bytes, timeout).await,
        Service::Operator => call_operator_service(connection, rpc, bytes, timeout).await,
        Service::Test => call_test_service(connection, rpc, bytes, timeout).await,
    }
}


'''

if __name__ == "__main__":
    raise SystemExit(main())
