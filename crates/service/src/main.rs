use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use anyhow::{Context, Result};
use interprocess::local_socket::{
    GenericFilePath, GenericNamespaced, ListenerOptions, Name, prelude::*,
};
use tokio::signal;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use tungsten_config::{
    BackendConfig, app_socket_path, app_state_path, backend_config_path, ensure_directory,
    ensure_parent,
};
use tungsten_service::{ConfigStore, Daemon};
use tungsten_ipc::{DEFAULT_SOCKET_NAME, IpcError, RequestMessage, read_frame, write_frame};
use tungsten_runtime::{Runtime, RuntimeConfig};
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path = backend_config_path()?;
    let settings = BackendConfig::load(&config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;
    let state_path = app_state_path()?;

    ensure_parent(&state_path)
        .with_context(|| format!("failed to prepare {}", state_path.display()))?;
    ensure_directory(&settings.temp_dir)
        .with_context(|| format!("failed to create {}", settings.temp_dir.display()))?;

    let runtime = Runtime::new(
        RuntimeConfig::new(
            state_path.clone(),
            settings.max_parallel,
            settings.connections,
        )
        .download_limit_kbps(settings.download_limit_kbps)
        .fallback_filename(settings.fallback_filename.clone())
        .temp_root(settings.temp_dir.clone()),
    )
    .with_context(|| format!("failed to initialize runtime at {}", state_path.display()))?;

    let _daemon = Daemon::new(
        Arc::new(runtime),
        ConfigStore::new(config_path, settings.clone()),
    );
    let socket_name = resolve_socket_name()?;
    let listener = ListenerOptions::new()
        .name(socket_name.borrow())
        .try_overwrite(true)
        .create_sync()
        .with_context(|| {
            format!(
                "failed to bind local socket {}",
                socket_display(&socket_name)
            )
        })?;

    info!(
        state_path = %state_path.display(),
        socket = %socket_display(&socket_name),
        max_parallel = settings.max_parallel,
        connections = settings.connections,
        "daemon initialized"
    );

    let daemon = _daemon.clone();
    let accept_loop = thread::spawn(move || run_listener(listener, daemon));

    signal::ctrl_c()
        .await
        .context("failed while waiting for shutdown signal")?;

    info!("shutdown signal received");
    drop(accept_loop);
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tungsten=debug"));

    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        warn!(error = %error, "failed to initialize tracing subscriber");
    }
}

fn run_listener(listener: interprocess::local_socket::Listener, daemon: Daemon) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                debug!(connection_id, "accepted ipc connection");
                let daemon = daemon.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_connection(connection_id, stream, daemon) {
                        error!(connection_id, error = %error, "connection handler failed");
                    }
                });
            }
            Err(error) => {
                error!(error = %error, "failed to accept incoming connection");
            }
        }
    }
}

fn handle_connection(
    connection_id: u64,
    mut stream: interprocess::local_socket::Stream,
    daemon: Daemon,
) -> Result<()> {
    loop {
        let message = match read_frame::<_, RequestMessage>(&mut stream) {
            Ok(message) => message,
            Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => {
                debug!(connection_id, "ipc connection closed by peer");
                return Ok(());
            }
            Err(error) => return Err(error).context("failed to read request frame"),
        };
        debug!(connection_id, ?message, "received ipc request");

        let handled = daemon.handle(message);
        debug!(
            connection_id,
            response = ?handled.response,
            subscription = handled.subscription.is_some(),
            "sending ipc response"
        );
        write_frame(&mut stream, &handled.response).context("failed to write response frame")?;

        let Some(subscription) = handled.subscription else {
            continue;
        };
        debug!(connection_id, "starting ipc subscription stream");

        loop {
            let event = match subscription.recv() {
                Ok(event) => event,
                Err(error) => return Err(error).context("subscription closed"),
            };
            debug!(connection_id, ?event, "sending ipc event");

            write_frame(&mut stream, &event).context("failed to write event frame")?;
        }
    }
}

fn resolve_socket_name() -> Result<Name<'static>> {
    if GenericNamespaced::is_supported() {
        return DEFAULT_SOCKET_NAME
            .to_ns_name::<GenericNamespaced>()
            .map(Name::into_owned)
            .context("failed to build namespaced local socket name");
    }

    let socket_path = app_socket_path()?;
    ensure_parent(&socket_path)
        .with_context(|| format!("failed to prepare {}", socket_path.display()))?;
    socket_path
        .to_string_lossy()
        .as_ref()
        .to_fs_name::<GenericFilePath>()
        .map(Name::into_owned)
        .with_context(|| {
            format!(
                "failed to build filesystem local socket name for {}",
                socket_path.display()
            )
        })
}

fn socket_display(name: &Name<'_>) -> String {
    format!("{name:?}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_display_is_not_empty() {
        let socket_name = if GenericNamespaced::is_supported() {
            DEFAULT_SOCKET_NAME
                .to_ns_name::<GenericNamespaced>()
                .expect("namespaced socket name should be valid")
        } else {
            "/tmp/tungsten-daemon.sock"
                .to_fs_name::<GenericFilePath>()
                .expect("filesystem socket name should be valid")
        };

        assert!(!socket_display(&socket_name).is_empty());
    }
}
