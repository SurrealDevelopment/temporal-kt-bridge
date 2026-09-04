//! Client connection.

use std::sync::Arc;
use std::time::Duration;

use temporalio_client::{Connection, ConnectionOptions};

use crate::error::{KtError, KtResult};

pub struct ClientEntry {
    pub connection: Connection,
    pub namespace: String,
}

/// Builds Core's connection options from the protobuf config.
///
/// Config arrives as protobuf rather than a `#[repr(C)]` struct, which is what removes the
/// 776-line hand-maintained Java layout the C bridge needed for this one message.
pub fn connection_options(config: &crate::proto::ClientOptions) -> KtResult<ConnectionOptions> {
    if config.target_url.is_empty() {
        return Err(KtError::InvalidArgument("target_url is empty".into()));
    }
    let target = url::Url::parse(&config.target_url)
        .map_err(|e| KtError::InvalidArgument(format!("target_url is not a URL: {e}")))?;

    // `bon`'s builder is typestate-based, so each setter returns a different type and the fields
    // cannot be applied conditionally with reassignment. The `maybe_*` setters take an Option and
    // keep the chain in one expression.
    //
    // client_name / client_version are deliberately absent: they are no longer part of
    // ConnectionOptions in SDK-Core 0.8, having moved to the higher-level client. They are still
    // in the proto so the JVM surface does not have to change when they are wired back up.
    Ok(ConnectionOptions::new(target)
        // Defaulted rather than required: Core rejects an empty identity when a worker is built,
        // and failing there is a confusing place to learn that a client option was missing.
        .identity(default_identity(&config.identity))
        .maybe_api_key(non_empty(&config.api_key))
        .maybe_connect_timeout(
            (config.connect_timeout_millis > 0)
                .then(|| Duration::from_millis(config.connect_timeout_millis)),
        )
        .build())
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

/// `<pid>@<hostname>`, the convention the other SDKs use.
fn default_identity(configured: &str) -> String {
    if !configured.is_empty() {
        return configured.to_string();
    }
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string());
    format!("{}@{}", std::process::id(), host)
}

pub async fn connect(
    options: ConnectionOptions,
    namespace: String,
) -> Result<Arc<ClientEntry>, String> {
    match Connection::connect(options).await {
        Ok(connection) => Ok(Arc::new(ClientEntry { connection, namespace })),
        Err(err) => Err(format!("{err:#}")),
    }
}
