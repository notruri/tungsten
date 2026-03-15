use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;
use tracing::warn;
use tungsten_core::{ConflictPolicy, DownloadId, DownloadRequest, IntegrityRule, ProgressSnapshot};

use super::{ControlSignal, TempLayout, TransferOutcome, TransferTask, TransferUpdate, Transport};

struct TestServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    range_gets: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn spawn(data: Vec<u8>, slow_body: bool, honor_range_gets: bool) -> Self {
        let chunk_delay = if slow_body {
            Duration::from_millis(5)
        } else {
            Duration::ZERO
        };
        Self::spawn_with_delay(data, chunk_delay, honor_range_gets)
    }

    fn spawn_with_delay(data: Vec<u8>, chunk_delay: Duration, honor_range_gets: bool) -> Self {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(value) => value,
            Err(error) => panic!("listener should bind: {error}"),
        };
        let addr = match listener.local_addr() {
            Ok(value) => value,
            Err(error) => panic!("listener should expose local addr: {error}"),
        };
        if let Err(error) = listener.set_nonblocking(true) {
            panic!("listener should become nonblocking: {error}");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let range_gets = Arc::new(AtomicUsize::new(0));
        let shared_data = Arc::new(data);
        let stop_flag = Arc::clone(&stop);
        let range_counter = Arc::clone(&range_gets);

        let handle = thread::spawn(move || {
            while !stop_flag.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let data = Arc::clone(&shared_data);
                        let range_gets = Arc::clone(&range_counter);
                        thread::spawn(move || {
                            if let Err(error) = handle_connection(
                                stream,
                                &data,
                                &range_gets,
                                chunk_delay,
                                honor_range_gets,
                            ) {
                                warn!(error = %error, "test server connection failed");
                            }
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => {
                        warn!(error = %error, "test server accept failed");
                        break;
                    }
                }
            }
        });

        Self {
            addr,
            stop,
            range_gets,
            handle: Some(handle),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }

    fn range_gets(&self) -> usize {
        self.range_gets.load(Ordering::SeqCst)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take()
            && let Err(error) = handle.join()
        {
            panic!("test server thread panicked: {error:?}");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reqwest_transfer_downloads_with_multiple_ranges() {
    let data = build_data(256 * 1024);
    let server = TestServer::spawn(data.clone(), false, true);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("download.part"));
    let transfer = Transport::new(4);
    let saw_multipart = Arc::new(AtomicBool::new(false));
    let saw_multipart_flag = Arc::clone(&saw_multipart);

    let outcome = match transfer
        .start(
            &task,
            None,
            &mut move |update: TransferUpdate| {
                if matches!(update.temp_layout, TempLayout::Multipart(_)) {
                    saw_multipart_flag.store(true, Ordering::SeqCst);
                }
                Ok(())
            },
            &|| ControlSignal::Run,
        )
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("multipart transfer should succeed: {error}"),
    };

    match outcome {
        TransferOutcome::Completed(update) => {
            assert!(matches!(update.temp_layout, TempLayout::Single));
            assert_eq!(update.progress.downloaded, data.len() as u64);
        }
        other => panic!("expected completion, got {other:?}"),
    }

    let file = match fs::read(&task.temp_path) {
        Ok(value) => value,
        Err(error) => panic!("downloaded file should be readable: {error}"),
    };
    assert_eq!(file, data);
    assert!(saw_multipart.load(Ordering::SeqCst));
    assert!(server.range_gets() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn reqwest_transfer_multipart_outlives_short_total_timeout() {
    let data = build_data(512 * 1024);
    let server = TestServer::spawn_with_delay(data.clone(), Duration::from_millis(25), true);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("timeout.part"));

    let timed_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(150))
        .build()
        .unwrap_or_else(|error| panic!("timed client should build: {error}"));
    let timed_transfer = Transport::with_client(timed_client, 4);
    let timed_error = match timed_transfer
        .start(&task, None, &mut |_update| Ok(()), &|| ControlSignal::Run)
        .await
    {
        Ok(outcome) => panic!("short-timeout transfer should fail, got {outcome:?}"),
        Err(error) => error,
    };
    assert!(matches!(timed_error, crate::error::NetError::Http(_)));

    let untimed_path = temp.path().join("untimed.part");
    let untimed_task = build_task(&server.url(), untimed_path.clone());
    let untimed_transfer = Transport::new(4);
    let outcome = match untimed_transfer
        .start(&untimed_task, None, &mut |_update| Ok(()), &|| {
            ControlSignal::Run
        })
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("untimed multipart transfer should succeed: {error}"),
    };

    match outcome {
        TransferOutcome::Completed(update) => {
            assert!(matches!(update.temp_layout, TempLayout::Single));
            assert_eq!(update.progress.downloaded, data.len() as u64);
        }
        other => panic!("expected completion, got {other:?}"),
    }

    let file = match fs::read(&untimed_path) {
        Ok(value) => value,
        Err(error) => panic!("untimed file should be readable: {error}"),
    };
    assert_eq!(file, data);
}

#[tokio::test(flavor = "current_thread")]
async fn reqwest_transfer_resumes_multipart_after_pause() {
    let data = build_data(512 * 1024);
    let server = TestServer::spawn(data.clone(), true, true);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("paused.part"));
    let transfer = Transport::new(4);
    let should_pause = Arc::new(AtomicBool::new(false));
    let pause_flag = Arc::clone(&should_pause);

    let paused = match transfer
        .start(
            &task,
            None,
            &mut move |update: TransferUpdate| {
                if update.progress.downloaded >= 128 * 1024 {
                    pause_flag.store(true, Ordering::SeqCst);
                }
                Ok(())
            },
            &|| {
                if should_pause.load(Ordering::SeqCst) {
                    ControlSignal::Pause
                } else {
                    ControlSignal::Run
                }
            },
        )
        .await
    {
        Ok(TransferOutcome::Paused(update)) => update,
        Ok(other) => panic!("expected pause, got {other:?}"),
        Err(error) => panic!("multipart pause should succeed: {error}"),
    };

    assert!(matches!(paused.temp_layout, TempLayout::Multipart(_)));
    assert!(paused.progress.downloaded > 0);

    let resumed_task = TransferTask {
        temp_layout: paused.temp_layout.clone(),
        ..task
    };
    let outcome = match transfer
        .start(&resumed_task, None, &mut |_update| Ok(()), &|| {
            ControlSignal::Run
        })
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("multipart resume should succeed: {error}"),
    };

    match outcome {
        TransferOutcome::Completed(update) => {
            assert!(matches!(update.temp_layout, TempLayout::Single));
            assert_eq!(update.progress.downloaded, data.len() as u64);
        }
        other => panic!("expected completion after resume, got {other:?}"),
    }

    let file = match fs::read(&resumed_task.temp_path) {
        Ok(value) => value,
        Err(error) => panic!("resumed file should be readable: {error}"),
    };
    assert_eq!(file, data);
}

#[tokio::test(flavor = "current_thread")]
async fn reqwest_transfer_falls_back_to_single_when_range_not_honored() {
    let data = build_data(256 * 1024);
    let server = TestServer::spawn(data.clone(), false, false);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("fallback.part"));
    let transfer = Transport::new(4);
    let saw_multipart = Arc::new(AtomicBool::new(false));
    let saw_single = Arc::new(AtomicBool::new(false));
    let saw_multipart_flag = Arc::clone(&saw_multipart);
    let saw_single_flag = Arc::clone(&saw_single);

    let outcome = match transfer
        .start(
            &task,
            None,
            &mut move |update: TransferUpdate| {
                if matches!(update.temp_layout, TempLayout::Multipart(_)) {
                    saw_multipart_flag.store(true, Ordering::SeqCst);
                }
                if matches!(update.temp_layout, TempLayout::Single) {
                    saw_single_flag.store(true, Ordering::SeqCst);
                }
                Ok(())
            },
            &|| ControlSignal::Run,
        )
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("fallback transfer should succeed: {error}"),
    };

    match outcome {
        TransferOutcome::Completed(update) => {
            assert!(matches!(update.temp_layout, TempLayout::Single));
            assert_eq!(update.progress.downloaded, data.len() as u64);
        }
        other => panic!("expected completion, got {other:?}"),
    }

    let file = match fs::read(&task.temp_path) {
        Ok(value) => value,
        Err(error) => panic!("fallback file should be readable: {error}"),
    };
    assert_eq!(file, data);
    assert!(saw_multipart.load(Ordering::SeqCst));
    assert!(saw_single.load(Ordering::SeqCst));
    assert!(server.range_gets() >= 1);
}

#[tokio::test(flavor = "current_thread")]
async fn reqwest_transfer_single_pause_keeps_preallocated_temp_size() {
    let data = build_data(256 * 1024);
    let server = TestServer::spawn(data.clone(), true, true);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("single-paused.part"));
    let transfer = Transport::new(1);
    let should_pause = Arc::new(AtomicBool::new(false));
    let pause_flag = Arc::clone(&should_pause);

    let paused = match transfer
        .start(
            &task,
            None,
            &mut move |update: TransferUpdate| {
                if update.progress.downloaded >= 64 * 1024 {
                    pause_flag.store(true, Ordering::SeqCst);
                }
                Ok(())
            },
            &|| {
                if should_pause.load(Ordering::SeqCst) {
                    ControlSignal::Pause
                } else {
                    ControlSignal::Run
                }
            },
        )
        .await
    {
        Ok(TransferOutcome::Paused(update)) => update,
        Ok(other) => panic!("expected pause, got {other:?}"),
        Err(error) => panic!("single pause should succeed: {error}"),
    };

    assert!(matches!(paused.temp_layout, TempLayout::Single));
    assert!(paused.progress.downloaded > 0);
    assert!((paused.progress.downloaded as usize) < data.len());

    let temp_len = match fs::metadata(&task.temp_path) {
        Ok(metadata) => metadata.len(),
        Err(error) => panic!("paused temp file metadata should be readable: {error}"),
    };
    assert_eq!(temp_len, data.len() as u64);
}

#[test]
fn capped_progress_uses_current_limit_for_speed_and_eta() {
    let mut speed_tracker = super::SpeedTracker::new(0, None);
    let progress = super::progress_from_metrics(
        1_100,
        Some(2_000),
        Duration::from_secs(11),
        &mut speed_tracker,
        Some(400),
    );

    assert_eq!(progress.speed_bps, Some(400));
    assert_eq!(progress.eta_seconds, Some(2));
}

#[test]
fn uncapped_progress_keeps_measured_speed_and_eta() {
    let mut speed_tracker = super::SpeedTracker::new(0, None);
    let progress = super::progress_from_metrics(
        1_100,
        Some(2_000),
        Duration::from_secs(11),
        &mut speed_tracker,
        None,
    );

    assert_eq!(progress.speed_bps, Some(100));
    assert_eq!(progress.eta_seconds, Some(9));
}

#[test]
fn speed_tracker_uses_resume_speed_until_new_samples_exist() {
    let mut speed_tracker = super::SpeedTracker::new(1_000, Some(100));
    let progress =
        super::progress_from_metrics(1_000, Some(2_000), Duration::ZERO, &mut speed_tracker, None);

    assert_eq!(progress.speed_bps, Some(100));
    assert_eq!(progress.eta_seconds, Some(10));
}

#[test]
fn eta_ema_smooths_resume_spike() {
    let mut speed_tracker = super::SpeedTracker::new(1_000, Some(100));
    let progress = super::progress_from_metrics(
        2_000,
        Some(5_000),
        Duration::from_secs(1),
        &mut speed_tracker,
        None,
    );

    assert_eq!(progress.speed_bps, Some(1_000));
    assert_eq!(progress.eta_seconds, Some(16));
}

#[test]
fn progress_for_speed_limit_recalculates_existing_snapshot() {
    let progress = super::progress_for_speed_limit(
        &ProgressSnapshot {
            downloaded: 1_000,
            total: Some(2_000),
            speed_bps: Some(100),
            eta_seconds: Some(10),
        },
        Some(500),
    );

    assert_eq!(progress.speed_bps, Some(500));
    assert_eq!(progress.eta_seconds, Some(2));
}

fn build_task(url: &str, temp_path: PathBuf) -> TransferTask {
    TransferTask {
        download_id: DownloadId(1),
        request: DownloadRequest::new(
            url.to_string(),
            temp_path.with_extension("bin"),
            ConflictPolicy::AutoRename,
            IntegrityRule::None,
        ),
        temp_path,
        temp_layout: TempLayout::Single,
        existing_size: 0,
        etag: Some("\"test-etag\"".to_string()),
        resume_speed_bps: None,
    }
}

fn build_data(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

fn handle_connection(
    mut stream: TcpStream,
    data: &[u8],
    range_gets: &AtomicUsize,
    chunk_delay: Duration,
    honor_range_gets: bool,
) -> Result<(), String> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let request = String::from_utf8(request).map_err(|error| error.to_string())?;
    let mut lines = request.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| "missing method".to_string())?;
    let _path = request_parts
        .next()
        .ok_or_else(|| "missing path".to_string())?;

    let mut range = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("range")
        {
            range = parse_range(value.trim(), data.len() as u64);
        }
    }

    match method {
        "HEAD" => write_response(&mut stream, 200, data, None, Duration::ZERO),
        "GET" => {
            let (status, body, content_range) = if let Some((start, end)) = range {
                range_gets.fetch_add(1, Ordering::SeqCst);
                if honor_range_gets {
                    let start_index = start as usize;
                    let end_index = end as usize + 1;
                    (
                        206,
                        &data[start_index..end_index],
                        Some(format!("bytes {start}-{end}/{}", data.len())),
                    )
                } else {
                    (200, data, None)
                }
            } else {
                (200, data, None)
            };

            write_response(
                &mut stream,
                status,
                body,
                content_range.as_deref(),
                chunk_delay,
            )
        }
        _ => write_response(&mut stream, 405, &[], None, Duration::ZERO),
    }
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    content_range: Option<&str>,
    chunk_delay: Duration,
) -> Result<(), String> {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        405 => "Method Not Allowed",
        _ => "Error",
    };

    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test-etag\"\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(content_range) = content_range {
        response.push_str(&format!("Content-Range: {content_range}\r\n"));
    }
    response.push_str("\r\n");

    stream
        .write_all(response.as_bytes())
        .map_err(|error| error.to_string())?;
    if !chunk_delay.is_zero() {
        for chunk in body.chunks(16 * 1024) {
            stream.write_all(chunk).map_err(|error| error.to_string())?;
            stream.flush().map_err(|error| error.to_string())?;
            thread::sleep(chunk_delay);
        }
    } else {
        stream.write_all(body).map_err(|error| error.to_string())?;
    }

    stream.flush().map_err(|error| error.to_string())
}

fn parse_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let bytes = value.strip_prefix("bytes=")?;
    let (start, end) = bytes.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() {
        total.checked_sub(1)?
    } else {
        end.parse::<u64>().ok()?
    };
    if start > end || end >= total {
        return None;
    }
    Some((start, end))
}
