use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

use crate::model::{ConflictPolicy, DownloadRequest, IntegrityRule};

use super::{
    ControlSignal, ReqwestTransfer, TempLayout, Transfer, TransferOutcome, TransferTask,
    TransferUpdate,
};

struct TestServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    range_gets: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn spawn(data: Vec<u8>, slow_body: bool) -> Self {
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
                            if let Err(error) =
                                handle_connection(stream, &data, &range_gets, slow_body)
                            {
                                eprintln!("test server connection failed: {error}");
                            }
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => {
                        eprintln!("test server accept failed: {error}");
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
        if let Some(handle) = self.handle.take() {
            if let Err(error) = handle.join() {
                panic!("test server thread panicked: {error:?}");
            }
        }
    }
}

#[test]
fn reqwest_transfer_downloads_with_multiple_ranges() {
    let data = build_data(256 * 1024);
    let server = TestServer::spawn(data.clone(), false);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("download.part"));
    let transfer = ReqwestTransfer::new(4);
    let saw_multipart = Arc::new(AtomicBool::new(false));
    let saw_multipart_flag = Arc::clone(&saw_multipart);

    let outcome = match transfer.download(
        &task,
        &mut move |update: TransferUpdate| {
            if matches!(update.temp_layout, TempLayout::Multipart(_)) {
                saw_multipart_flag.store(true, Ordering::SeqCst);
            }
            Ok(())
        },
        &|| ControlSignal::Run,
    ) {
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

#[test]
fn reqwest_transfer_resumes_multipart_after_pause() {
    let data = build_data(512 * 1024);
    let server = TestServer::spawn(data.clone(), true);
    let temp = match tempdir() {
        Ok(value) => value,
        Err(error) => panic!("tempdir should be created: {error}"),
    };
    let task = build_task(&server.url(), temp.path().join("paused.part"));
    let transfer = ReqwestTransfer::new(4);
    let should_pause = Arc::new(AtomicBool::new(false));
    let pause_flag = Arc::clone(&should_pause);

    let paused = match transfer.download(
        &task,
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
    ) {
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
    let outcome =
        match transfer.download(&resumed_task, &mut |_update| Ok(()), &|| ControlSignal::Run) {
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

fn build_task(url: &str, temp_path: PathBuf) -> TransferTask {
    TransferTask {
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
    }
}

fn build_data(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

fn handle_connection(
    mut stream: TcpStream,
    data: &[u8],
    range_gets: &AtomicUsize,
    slow_body: bool,
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
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("range") {
                range = parse_range(value.trim(), data.len() as u64);
            }
        }
    }

    match method {
        "HEAD" => write_response(&mut stream, 200, data, None, false),
        "GET" => {
            let (status, body, content_range) = if let Some((start, end)) = range {
                range_gets.fetch_add(1, Ordering::SeqCst);
                let start_index = start as usize;
                let end_index = end as usize + 1;
                (
                    206,
                    &data[start_index..end_index],
                    Some(format!("bytes {start}-{end}/{}", data.len())),
                )
            } else {
                (200, data, None)
            };

            write_response(
                &mut stream,
                status,
                body,
                content_range.as_deref(),
                slow_body,
            )
        }
        _ => write_response(&mut stream, 405, &[], None, false),
    }
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    content_range: Option<&str>,
    slow_body: bool,
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
    if slow_body {
        for chunk in body.chunks(16 * 1024) {
            stream.write_all(chunk).map_err(|error| error.to_string())?;
            stream.flush().map_err(|error| error.to_string())?;
            thread::sleep(Duration::from_millis(5));
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
