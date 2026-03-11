use tungsten_net::NetError;
use tungsten_net::model::{ConflictPolicy, DownloadStatus, IntegrityRule};
use tungsten_net::transfer::{MultipartPart, MultipartState, TempLayout};

pub(super) fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

pub(super) fn i64_to_bool(value: i64, field: &str) -> Result<bool, NetError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(NetError::State(format!(
            "invalid boolean value for {field}: {value}"
        ))),
    }
}

pub(super) fn u64_to_i64(value: u64, field: &str) -> Result<i64, NetError> {
    i64::try_from(value)
        .map_err(|error| NetError::State(format!("value out of range for {field}: {error}")))
}

pub(super) fn usize_to_i64(value: usize, field: &str) -> Result<i64, NetError> {
    i64::try_from(value)
        .map_err(|error| NetError::State(format!("value out of range for {field}: {error}")))
}

pub(super) fn i64_to_u64(value: i64, field: &str) -> Result<u64, NetError> {
    u64::try_from(value)
        .map_err(|error| NetError::State(format!("negative value for {field}: {error}")))
}

pub(super) fn encode_conflict_policy(conflict: &ConflictPolicy) -> &'static str {
    match conflict {
        ConflictPolicy::AutoRename => "auto_rename",
    }
}

pub(super) fn decode_conflict_policy(value: &str) -> Result<ConflictPolicy, NetError> {
    match value {
        "auto_rename" => Ok(ConflictPolicy::AutoRename),
        _ => Err(NetError::State(format!("invalid conflict policy: {value}"))),
    }
}

pub(super) fn encode_integrity_rule(rule: &IntegrityRule) -> (&'static str, Option<&str>) {
    match rule {
        IntegrityRule::None => ("none", None),
        IntegrityRule::Sha256(hash) => ("sha256", Some(hash.as_str())),
    }
}

pub(super) fn decode_integrity_rule(
    kind: &str,
    value: Option<&str>,
) -> Result<IntegrityRule, NetError> {
    match kind {
        "none" => Ok(IntegrityRule::None),
        "sha256" => value
            .map(|hash| IntegrityRule::Sha256(hash.to_string()))
            .ok_or_else(|| NetError::State("missing sha256 value for integrity rule".to_string())),
        _ => Err(NetError::State(format!(
            "invalid integrity rule kind: {kind}"
        ))),
    }
}

pub(super) fn encode_download_status(status: &DownloadStatus) -> &'static str {
    match status {
        DownloadStatus::Queued => "queued",
        DownloadStatus::Running => "running",
        DownloadStatus::Paused => "paused",
        DownloadStatus::Verifying => "verifying",
        DownloadStatus::Completed => "completed",
        DownloadStatus::Failed => "failed",
        DownloadStatus::Cancelled => "cancelled",
    }
}

pub(super) fn decode_download_status(value: &str) -> Result<DownloadStatus, NetError> {
    match value {
        "queued" => Ok(DownloadStatus::Queued),
        "running" => Ok(DownloadStatus::Running),
        "paused" => Ok(DownloadStatus::Paused),
        "verifying" => Ok(DownloadStatus::Verifying),
        "completed" => Ok(DownloadStatus::Completed),
        "failed" => Ok(DownloadStatus::Failed),
        "cancelled" => Ok(DownloadStatus::Cancelled),
        _ => Err(NetError::State(format!("invalid download status: {value}"))),
    }
}

pub(super) fn encode_temp_layout(
    layout: &TempLayout,
) -> Result<(&'static str, Option<i64>), NetError> {
    match layout {
        TempLayout::Single => Ok(("single", None)),
        TempLayout::Multipart(multipart) => Ok((
            "multipart",
            Some(u64_to_i64(
                multipart.total_size,
                "downloads.temp_layout_total_size",
            )?),
        )),
    }
}

pub(super) fn decode_temp_layout(
    kind: &str,
    total_size: Option<i64>,
    parts: Vec<MultipartPart>,
) -> Result<TempLayout, NetError> {
    match kind {
        "single" => Ok(TempLayout::Single),
        "multipart" => {
            let total_size = total_size
                .ok_or_else(|| {
                    NetError::State(
                        "missing temp_layout_total_size for multipart download".to_string(),
                    )
                })
                .and_then(|value| i64_to_u64(value, "downloads.temp_layout_total_size"))?;
            Ok(TempLayout::Multipart(MultipartState { total_size, parts }))
        }
        _ => Err(NetError::State(format!("invalid temp layout kind: {kind}"))),
    }
}
