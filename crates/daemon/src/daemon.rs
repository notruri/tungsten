use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};

use tungsten_ipc::AppConfig;
use tungsten_ipc::{
    Event, EventMessage, QueueNotification, RemoteError, Request, RequestMessage, Response,
    ResponseMessage,
};
use tungsten_runtime::{Runtime, RuntimeError};

#[derive(Clone)]
pub struct Daemon {
    runtime: Arc<Runtime>,
    config: ConfigStore,
}

impl Daemon {
    pub fn new(runtime: Arc<Runtime>, config: ConfigStore) -> Self {
        Self { runtime, config }
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub fn handle(&self, message: RequestMessage) -> HandleResult {
        let RequestMessage { id, request } = message;

        let outcome = match request {
            Request::Ping => Ok(HandleOutcome {
                response: Response::Pong,
                subscription: None,
            }),
            Request::GetConfig => self.handle_get_config(),
            Request::SetConfig { config } => self.handle_set_config(config),
            Request::Snapshot => self.handle_snapshot(),
            Request::Subscribe => self.handle_subscribe(),
            Request::Enqueue { request } => self.handle_enqueue(request),
            Request::Pause { download_id } => self.handle_ack(|runtime| runtime.pause(download_id)),
            Request::Resume { download_id } => {
                self.handle_ack(|runtime| runtime.resume(download_id))
            }
            Request::Cancel { download_id } => {
                self.handle_ack(|runtime| runtime.cancel(download_id))
            }
            Request::Remove { download_id } => {
                self.handle_ack(|runtime| runtime.remove(download_id))
            }
            Request::SetConnections { connections } => {
                self.handle_ack(|runtime| runtime.set_connections(connections))
            }
            Request::SetDownloadLimit {
                download_limit_kbps,
            } => self.handle_ack(|runtime| runtime.set_download_limit(download_limit_kbps)),
            Request::SetSpeedLimit {
                download_id,
                speed_limit_kbps,
            } => self.handle_ack(|runtime| runtime.set_speed_limit(download_id, speed_limit_kbps)),
            Request::SetFallbackFilename { fallback_filename } => {
                self.handle_ack(|runtime| runtime.set_fallback_filename(fallback_filename))
            }
            Request::SetMaxParallel { max_parallel } => {
                self.handle_ack(|runtime| runtime.set_max_parallel(max_parallel))
            }
            Request::SetTempRoot { temp_root } => {
                self.handle_ack(|runtime| runtime.set_temp_root(temp_root))
            }
        };

        let HandleOutcome {
            response,
            subscription,
        } = match outcome {
            Ok(outcome) => outcome,
            Err(error) => HandleOutcome {
                response: Response::Error(RemoteError::from(error)),
                subscription: None,
            },
        };

        HandleResult {
            response: ResponseMessage { id, response },
            subscription,
        }
    }

    fn handle_snapshot(&self) -> Result<HandleOutcome, RuntimeError> {
        let downloads = self.runtime.snapshot()?;
        Ok(HandleOutcome {
            response: Response::Snapshot { downloads },
            subscription: None,
        })
    }

    fn handle_get_config(&self) -> Result<HandleOutcome, RuntimeError> {
        let config = self.config.current()?;
        Ok(HandleOutcome {
            response: Response::Config { config },
            subscription: None,
        })
    }

    fn handle_set_config(&self, config: AppConfig) -> Result<HandleOutcome, RuntimeError> {
        let config = config.normalize();
        config.validate()?;

        self.runtime.set_max_parallel(config.max_parallel)?;
        self.runtime.set_connections(config.connections)?;
        self.runtime
            .set_download_limit(config.download_limit_kbps)?;
        self.runtime
            .set_fallback_filename(config.fallback_filename.clone())?;
        self.runtime.set_temp_root(config.temp_dir.clone())?;
        self.config.save(config)?;

        Ok(HandleOutcome {
            response: Response::Ack,
            subscription: None,
        })
    }

    fn handle_subscribe(&self) -> Result<HandleOutcome, RuntimeError> {
        let receiver = self.runtime.subscribe()?;
        Ok(HandleOutcome {
            response: Response::Subscribed,
            subscription: Some(Subscription::new(receiver)),
        })
    }

    fn handle_enqueue(
        &self,
        request: tungsten_runtime::DownloadRequest,
    ) -> Result<HandleOutcome, RuntimeError> {
        let download_id = self.runtime.enqueue(request)?;
        Ok(HandleOutcome {
            response: Response::Enqueued { download_id },
            subscription: None,
        })
    }

    fn handle_ack<F>(&self, action: F) -> Result<HandleOutcome, RuntimeError>
    where
        F: FnOnce(&Runtime) -> Result<(), RuntimeError>,
    {
        action(&self.runtime)?;
        Ok(HandleOutcome {
            response: Response::Ack,
            subscription: None,
        })
    }
}

struct HandleOutcome {
    response: Response,
    subscription: Option<Subscription>,
}

pub struct HandleResult {
    pub response: ResponseMessage,
    pub subscription: Option<Subscription>,
}

pub struct Subscription {
    receiver: mpsc::Receiver<tungsten_runtime::QueueEvent>,
}

impl Subscription {
    pub fn new(receiver: mpsc::Receiver<tungsten_runtime::QueueEvent>) -> Self {
        Self { receiver }
    }

    pub fn recv(&self) -> Result<EventMessage, mpsc::RecvError> {
        self.receiver.recv().map(queue_event_message)
    }

    pub fn try_recv(&self) -> Result<EventMessage, mpsc::TryRecvError> {
        self.receiver.try_recv().map(queue_event_message)
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<EventMessage, mpsc::RecvTimeoutError> {
        self.receiver.recv_timeout(timeout).map(queue_event_message)
    }
}

fn queue_event_message(event: tungsten_runtime::QueueEvent) -> EventMessage {
    EventMessage {
        event: Event::Queue {
            event: QueueNotification::from(event),
        },
    }
}

#[derive(Clone)]
pub struct ConfigStore {
    path: PathBuf,
    current: Arc<Mutex<AppConfig>>,
}

impl ConfigStore {
    pub fn new(path: PathBuf, initial: AppConfig) -> Self {
        Self {
            path,
            current: Arc::new(Mutex::new(initial)),
        }
    }

    pub fn current(&self) -> Result<AppConfig, RuntimeError> {
        self.current
            .lock()
            .map(|guard| guard.clone())
            .map_err(|error| RuntimeError::State(format!("config lock poisoned: {error}")))
    }

    pub fn save(&self, config: AppConfig) -> Result<(), RuntimeError> {
        let content = toml::to_string_pretty(&config)
            .map_err(|error| RuntimeError::Backend(format!("failed to encode config: {error}")))?;

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, content)?;

        let mut guard = self
            .current
            .lock()
            .map_err(|error| RuntimeError::State(format!("config lock poisoned: {error}")))?;
        *guard = config;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use tempfile::TempDir;
    use tungsten_ipc::{AppConfig, MessageId, Request, RequestMessage, ThemePreference};
    use tungsten_runtime::{ConflictPolicy, DownloadRequest, IntegrityRule, RuntimeConfig};

    use super::*;

    struct TestDaemon {
        _dir: TempDir,
        daemon: Daemon,
    }

    fn test_daemon() -> TestDaemon {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let runtime = Runtime::new(
            RuntimeConfig::new(dir.path().join("state.db"), 2, 2).temp_root(dir.path().join("tmp")),
        )
        .expect("runtime should be created");
        let config = AppConfig {
            download_root: dir.path().join("downloads"),
            temp_dir: dir.path().join("tmp"),
            fallback_filename: "download.bin".to_string(),
            max_parallel: 2,
            connections: 2,
            download_limit_kbps: 0,
            minimize_to_tray: false,
            theme: ThemePreference::System,
        };

        TestDaemon {
            _dir: dir,
            daemon: Daemon::new(
                Arc::new(runtime),
                ConfigStore::new(PathBuf::from("config.toml"), config),
            ),
        }
    }

    #[test]
    fn handle_ping_returns_pong() {
        let fixture = test_daemon();
        let result = fixture.daemon.handle(RequestMessage {
            id: MessageId(1),
            request: Request::Ping,
        });

        assert!(matches!(result.response.response, Response::Pong));
        assert!(result.subscription.is_none());
    }

    #[test]
    fn handle_enqueue_returns_download_id() {
        let fixture = test_daemon();
        let request = DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            PathBuf::from("downloads/file.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        );
        let result = fixture.daemon.handle(RequestMessage {
            id: MessageId(7),
            request: Request::Enqueue { request },
        });

        match result.response.response {
            Response::Enqueued { download_id } => assert_eq!(download_id.0, 1),
            _ => panic!("expected enqueue response"),
        }
    }

    #[test]
    fn handle_subscribe_provides_queue_events() {
        let fixture = test_daemon();
        let subscription = fixture
            .daemon
            .handle(RequestMessage {
                id: MessageId(2),
                request: Request::Subscribe,
            })
            .subscription
            .expect("subscribe should return receiver");

        let request = DownloadRequest::new(
            "https://example.com/file.bin".to_string(),
            PathBuf::from("downloads/file.bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        );
        let _ = fixture.daemon.handle(RequestMessage {
            id: MessageId(3),
            request: Request::Enqueue { request },
        });

        let event = subscription
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("event should arrive");
        match event.event {
            Event::Queue {
                event: QueueNotification::Added { record },
            } => {
                assert_eq!(record.id.0, 1);
            }
            _ => panic!("expected added event"),
        }
    }

    #[test]
    fn handle_get_config_returns_config() {
        let fixture = test_daemon();
        let result = fixture.daemon.handle(RequestMessage {
            id: MessageId(4),
            request: Request::GetConfig,
        });

        match result.response.response {
            Response::Config { config } => {
                assert_eq!(config.max_parallel, 2);
                assert_eq!(config.connections, 2);
            }
            _ => panic!("expected config response"),
        }
    }
}
