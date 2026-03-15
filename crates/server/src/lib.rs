use std::convert::Infallible;
use std::net::SocketAddr;

use axum::extract::{Query, State};
use axum::http::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, body::Body};
use serde::Deserialize;
use tracing::info;

const DEFAULT_ADDR: &str = "127.0.0.1:18080";
const CHUNK_SIZE: usize = 64 * 1024;

#[derive(Clone, Copy)]
struct RouteMode {
    supports_ranges: bool,
}

#[derive(Debug, Deserialize)]
struct SizeQuery {
    size: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    end: u64,
}

pub async fn run(addr: Option<SocketAddr>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = addr.unwrap_or(DEFAULT_ADDR.parse::<SocketAddr>()?);
    let app = app();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(?addr, "starting server");
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn app() -> Router {
    Router::new()
        .route(
            "/single",
            get(file_handler).with_state(RouteMode {
                supports_ranges: false,
            }),
        )
        .route(
            "/multi",
            get(file_handler).with_state(RouteMode {
                supports_ranges: true,
            }),
        )
}

async fn file_handler(
    State(mode): State<RouteMode>,
    Query(query): Query<SizeQuery>,
    headers: HeaderMap,
) -> Response {
    let total_size = match parse_size(&query.size) {
        Ok(value) => value,
        Err(error) => return bad_request(error),
    };

    let parsed_range = headers
        .get(axum::http::header::RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| parse_range(value, total_size));

    let effective_range = if mode.supports_ranges {
        parsed_range
    } else {
        None
    };
    let status = if effective_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let body_range = effective_range.unwrap_or(ByteRange {
        start: 0,
        end: total_size.saturating_sub(1),
    });
    let body_len = body_range.end - body_range.start + 1;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(ETAG, HeaderValue::from_static("\"tungsten-test-etag\""));
    response_headers.insert(
        CONTENT_LENGTH,
        header_u64(body_len).unwrap_or_else(|_| HeaderValue::from_static("0")),
    );

    if mode.supports_ranges {
        response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        if let Some(range) = effective_range {
            let content_range = format!("bytes {}-{}/{}", range.start, range.end, total_size);
            if let Ok(value) = HeaderValue::from_str(&content_range) {
                response_headers.insert(CONTENT_RANGE, value);
            }
        }
    } else {
        response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("none"));
    }

    let stream = futures_util::stream::unfold(body_range, move |range| async move {
        if range.start > range.end {
            return None;
        }

        let remaining = range.end - range.start + 1;
        let chunk_len = remaining.min(CHUNK_SIZE as u64) as usize;
        let mut chunk = vec![0u8; chunk_len];
        fill_chunk(range.start, &mut chunk);

        let next_start = range.start + chunk_len as u64;
        let next_range = ByteRange {
            start: next_start,
            end: range.end,
        };
        Some((
            Ok::<_, Infallible>(axum::body::Bytes::from(chunk)),
            next_range,
        ))
    });

    (status, response_headers, Body::from_stream(stream)).into_response()
}

fn parse_size(input: &str) -> Result<u64, &'static str> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("size is required");
    }

    let split_at = trimmed
        .find(|value: char| !value.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number_part, suffix_part) = trimmed.split_at(split_at);
    if number_part.is_empty() {
        return Err("size must start with digits");
    }

    let number = number_part
        .parse::<u64>()
        .map_err(|_| "size number is invalid")?;

    let suffix = suffix_part.trim().to_ascii_uppercase();
    let multiplier = match suffix.as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KI" | "KIB" => 1024,
        "M" | "MB" | "MI" | "MIB" => 1024_u64.pow(2),
        "G" | "GB" | "GI" | "GIB" => 1024_u64.pow(3),
        "T" | "TB" | "TI" | "TIB" => 1024_u64.pow(4),
        _ => return Err("unsupported size suffix"),
    };

    number
        .checked_mul(multiplier)
        .ok_or("size value is too large")
}

fn parse_range(input: &str, total_size: u64) -> Option<ByteRange> {
    if total_size == 0 {
        return None;
    }

    let bytes = input.strip_prefix("bytes=")?;
    let (start_raw, end_raw) = bytes.split_once('-')?;
    let start = start_raw.parse::<u64>().ok()?;
    let end = if end_raw.is_empty() {
        total_size.checked_sub(1)?
    } else {
        end_raw.parse::<u64>().ok()?
    };

    if start > end || end >= total_size {
        return None;
    }

    Some(ByteRange { start, end })
}

fn header_u64(value: u64) -> Result<HeaderValue, axum::http::header::InvalidHeaderValue> {
    HeaderValue::from_str(&value.to_string())
}

fn fill_chunk(start_offset: u64, chunk: &mut [u8]) {
    for (index, slot) in chunk.iter_mut().enumerate() {
        let absolute = start_offset + index as u64;
        *slot = (absolute % 251) as u8;
    }
}

fn bad_request(message: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, message).into_response()
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, parse_range, parse_size};

    #[test]
    fn parse_size_supports_binary_suffix() {
        assert_eq!(parse_size("5G"), Ok(5 * 1024_u64.pow(3)));
        assert_eq!(parse_size("64MiB"), Ok(64 * 1024_u64.pow(2)));
        assert_eq!(parse_size("1024"), Ok(1024));
    }

    #[test]
    fn parse_size_rejects_unknown_suffix() {
        assert!(parse_size("1P").is_err());
        assert!(parse_size("abc").is_err());
    }

    #[test]
    fn parse_range_parses_open_and_closed_ranges() {
        assert_eq!(
            parse_range("bytes=10-20", 100),
            Some(ByteRange { start: 10, end: 20 })
        );
        assert_eq!(
            parse_range("bytes=10-", 100),
            Some(ByteRange { start: 10, end: 99 })
        );
    }

    #[test]
    fn parse_range_rejects_invalid_bounds() {
        assert_eq!(parse_range("bytes=20-10", 100), None);
        assert_eq!(parse_range("bytes=0-100", 100), None);
        assert_eq!(parse_range("invalid", 100), None);
    }
}
