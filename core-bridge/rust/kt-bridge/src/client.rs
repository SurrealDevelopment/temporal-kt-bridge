//! Client connection.

use std::sync::Arc;
use std::time::Duration;

use temporalio_client::GrpcCompression;
use temporalio_client::{ClientTlsOptions, TlsOptions};
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
    // client-name / client-version go on every RPC as headers. Core would otherwise stamp its
    // own ("temporal-rust" / the Core crate version), and servers key behaviour on these: the Java
    // time-skipping test server answers a woken long poll for an unrecognised client with a bare
    // UNKNOWN. Sent through the headers map because the dedicated setters are gated behind Core's
    // `core-based-sdk` feature; the header interceptor honours a value that is already present.
    let mut headers = std::collections::HashMap::new();
    if !config.client_name.is_empty() {
        headers.insert("client-name".to_string(), config.client_name.clone());
    }
    if !config.client_version.is_empty() {
        headers.insert("client-version".to_string(), config.client_version.clone());
    }
    // Any TLS field, or the bare flag, turns TLS on. An https:// target with tls_options None is
    // refused by tonic outright, which is how the previous version of this made Temporal Cloud
    // unreachable while every local test kept passing.
    let wants_tls = config.tls
        || !config.server_root_ca_cert.is_empty()
        || !config.client_cert.is_empty()
        || !config.client_private_key.is_empty()
        || !config.tls_domain.is_empty();
    let tls_options = if wants_tls {
        let client_tls = (!config.client_cert.is_empty() || !config.client_private_key.is_empty())
            .then(|| {
                ClientTlsOptions::builder()
                    .client_cert(config.client_cert.clone())
                    .client_private_key(config.client_private_key.clone())
                    .build()
            });
        Some(
            TlsOptions::builder()
                .maybe_server_root_ca_cert(
                    (!config.server_root_ca_cert.is_empty())
                        .then(|| config.server_root_ca_cert.clone()),
                )
                .maybe_domain(non_empty(&config.tls_domain))
                .maybe_client_tls_options(client_tls)
                .build(),
        )
    } else {
        None
    };

    Ok(ConnectionOptions::new(target)
        .maybe_tls_options(tls_options)
        .maybe_headers((!headers.is_empty()).then_some(headers))
        // Defaulted rather than required: Core rejects an empty identity when a worker is built,
        // and failing there is a confusing place to learn that a client option was missing.
        .identity(default_identity(&config.identity))
        .grpc_compression(if config.no_compression {
            GrpcCompression::None
        } else {
            GrpcCompression::Gzip
        })
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
    // A real syscall, not $HOSTNAME: that is a shell variable and is not exported, so reading the
    // environment produced "unknown-host" on macOS and in most containers.
    let host = gethostname::gethostname().to_string_lossy().into_owned();
    let host = if host.is_empty() {
        "unknown-host".to_string()
    } else {
        host
    };
    format!("{}@{}", std::process::id(), host)
}

pub async fn connect(
    options: ConnectionOptions,
    namespace: String,
) -> Result<Arc<ClientEntry>, String> {
    match Connection::connect(options).await {
        Ok(connection) => Ok(Arc::new(ClientEntry {
            connection,
            namespace,
        })),
        Err(err) => Err(format!("{err:#}")),
    }
}

#[cfg(test)]
mod tls_tests {
    use super::*;

    fn base() -> crate::proto::ClientOptions {
        crate::proto::ClientOptions {
            target_url: "https://cloud.example:7233".into(),
            namespace: "ns".into(),
            ..Default::default()
        }
    }

    #[test]
    fn plain_http_has_no_tls() {
        let mut cfg = base();
        cfg.target_url = "http://localhost:7233".into();
        let options = connection_options(&cfg).expect("options");
        assert!(
            options.tls_options.is_none(),
            "an http:// target must not turn TLS on"
        );
    }

    #[test]
    fn the_bare_flag_turns_tls_on_with_system_roots() {
        let mut cfg = base();
        cfg.tls = true;
        let options = connection_options(&cfg).expect("options");
        let tls = options.tls_options.expect("tls on");
        assert!(tls.server_root_ca_cert.is_none());
        assert!(tls.client_tls_options.is_none());
    }

    #[test]
    fn material_is_carried_through() {
        let mut cfg = base();
        cfg.server_root_ca_cert = vec![1, 2, 3];
        cfg.tls_domain = "cloud.example".into();
        cfg.client_cert = vec![4];
        cfg.client_private_key = vec![5, 6];
        let tls = connection_options(&cfg)
            .expect("options")
            .tls_options
            .expect("tls on");
        assert_eq!(tls.server_root_ca_cert.as_deref(), Some(&[1u8, 2, 3][..]));
        assert_eq!(tls.domain.as_deref(), Some("cloud.example"));
        let client = tls.client_tls_options.expect("mTLS");
        assert_eq!(client.client_cert, vec![4]);
        assert_eq!(client.client_private_key, vec![5, 6]);
    }
}
