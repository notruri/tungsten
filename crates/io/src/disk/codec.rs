use chrono::{DateTime, Utc};
use tungsten_core::CoreError;
use tungsten_core::{ConflictPolicy, DownloadRequest, DownloadStatus, IntegrityRule};
use tungsten_core::{MultipartPart, MultipartState, TempLayout};

pub(super) trait ToDatabase {
    type Output;

    fn to_db(&self, field: &str) -> Result<Self::Output, CoreError>;
}

pub(super) trait FromDatabase<Input>: Sized {
    fn from_db(value: Input, field: &str) -> Result<Self, CoreError>;
}

pub(super) struct IntegrityValue {
    pub kind: String,
    pub value: Option<String>,
}

pub(super) struct RequestValue {
    pub url: String,
    pub destination: String,
    pub conflict: String,
    pub integrity_kind: String,
    pub integrity_value: Option<String>,
    pub speed_limit_kbps: Option<i64>,
}

pub(super) struct TempLayoutValue {
    pub kind: String,
    pub total_size: Option<i64>,
}

impl ToDatabase for bool {
    type Output = i64;

    fn to_db(&self, _field: &str) -> Result<Self::Output, CoreError> {
        Ok(if *self { 1 } else { 0 })
    }
}

impl FromDatabase<i64> for bool {
    fn from_db(value: i64, field: &str) -> Result<Self, CoreError> {
        match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CoreError::State(format!(
                "invalid boolean value for {field}: {value}"
            ))),
        }
    }
}

impl ToDatabase for u64 {
    type Output = i64;

    fn to_db(&self, field: &str) -> Result<Self::Output, CoreError> {
        i64::try_from(*self)
            .map_err(|error| CoreError::State(format!("value out of range for {field}: {error}")))
    }
}

impl FromDatabase<i64> for u64 {
    fn from_db(value: i64, field: &str) -> Result<Self, CoreError> {
        u64::try_from(value)
            .map_err(|error| CoreError::State(format!("negative value for {field}: {error}")))
    }
}

impl ToDatabase for usize {
    type Output = i64;

    fn to_db(&self, field: &str) -> Result<Self::Output, CoreError> {
        i64::try_from(*self)
            .map_err(|error| CoreError::State(format!("value out of range for {field}: {error}")))
    }
}

impl ToDatabase for DateTime<Utc> {
    type Output = String;

    fn to_db(&self, _field: &str) -> Result<Self::Output, CoreError> {
        Ok(self.to_rfc3339())
    }
}

impl FromDatabase<String> for DateTime<Utc> {
    fn from_db(value: String, field: &str) -> Result<Self, CoreError> {
        DateTime::parse_from_rfc3339(&value)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|error| CoreError::State(format!("invalid datetime for {field}: {error}")))
    }
}

impl ToDatabase for ConflictPolicy {
    type Output = String;

    fn to_db(&self, _field: &str) -> Result<Self::Output, CoreError> {
        let value = match self {
            ConflictPolicy::AutoRename => "auto_rename",
        };
        Ok(value.to_string())
    }
}

impl FromDatabase<String> for ConflictPolicy {
    fn from_db(value: String, field: &str) -> Result<Self, CoreError> {
        match value.as_str() {
            "auto_rename" => Ok(ConflictPolicy::AutoRename),
            _ => Err(CoreError::State(format!(
                "invalid conflict policy for {field}: {value}"
            ))),
        }
    }
}

impl ToDatabase for IntegrityRule {
    type Output = IntegrityValue;

    fn to_db(&self, _field: &str) -> Result<Self::Output, CoreError> {
        let value = match self {
            IntegrityRule::None => IntegrityValue {
                kind: "none".to_string(),
                value: None,
            },
            IntegrityRule::Sha256(hash) => IntegrityValue {
                kind: "sha256".to_string(),
                value: Some(hash.clone()),
            },
        };
        Ok(value)
    }
}

impl FromDatabase<IntegrityValue> for IntegrityRule {
    fn from_db(value: IntegrityValue, field: &str) -> Result<Self, CoreError> {
        match value.kind.as_str() {
            "none" => Ok(IntegrityRule::None),
            "sha256" => value
                .value
                .map(IntegrityRule::Sha256)
                .ok_or_else(|| CoreError::State(format!("missing sha256 value for {field}"))),
            _ => Err(CoreError::State(format!(
                "invalid integrity rule kind for {field}: {}",
                value.kind
            ))),
        }
    }
}

impl ToDatabase for DownloadStatus {
    type Output = String;

    fn to_db(&self, _field: &str) -> Result<Self::Output, CoreError> {
        let value = match self {
            DownloadStatus::Queued => "queued",
            DownloadStatus::Preparing => "preparing",
            DownloadStatus::Running => "running",
            DownloadStatus::Finalizing => "finalizing",
            DownloadStatus::Paused => "paused",
            DownloadStatus::Verifying => "verifying",
            DownloadStatus::Completed => "completed",
            DownloadStatus::Failed => "failed",
            DownloadStatus::Cancelled => "cancelled",
        };
        Ok(value.to_string())
    }
}

impl FromDatabase<String> for DownloadStatus {
    fn from_db(value: String, field: &str) -> Result<Self, CoreError> {
        match value.as_str() {
            "queued" => Ok(DownloadStatus::Queued),
            "preparing" => Ok(DownloadStatus::Preparing),
            "running" => Ok(DownloadStatus::Running),
            "finalizing" => Ok(DownloadStatus::Finalizing),
            "paused" => Ok(DownloadStatus::Paused),
            "verifying" => Ok(DownloadStatus::Verifying),
            "completed" => Ok(DownloadStatus::Completed),
            "failed" => Ok(DownloadStatus::Failed),
            "cancelled" => Ok(DownloadStatus::Cancelled),
            _ => Err(CoreError::State(format!(
                "invalid download status for {field}: {value}"
            ))),
        }
    }
}

impl ToDatabase for DownloadRequest {
    type Output = RequestValue;

    fn to_db(&self, field: &str) -> Result<Self::Output, CoreError> {
        let integrity = self.integrity.to_db(field)?;
        Ok(RequestValue {
            url: self.url.clone(),
            destination: self.destination.to_string_lossy().into_owned(),
            conflict: self.conflict.to_db(field)?,
            integrity_kind: integrity.kind,
            integrity_value: integrity.value,
            speed_limit_kbps: self
                .speed_limit_kbps
                .map(|value| value.to_db(field))
                .transpose()?,
        })
    }
}

impl FromDatabase<RequestValue> for DownloadRequest {
    fn from_db(value: RequestValue, field: &str) -> Result<Self, CoreError> {
        Ok(DownloadRequest {
            url: value.url,
            destination: value.destination.into(),
            conflict: ConflictPolicy::from_db(value.conflict, field)?,
            integrity: IntegrityRule::from_db(
                IntegrityValue {
                    kind: value.integrity_kind,
                    value: value.integrity_value,
                },
                field,
            )?,
            speed_limit_kbps: value
                .speed_limit_kbps
                .map(|value| u64::from_db(value, field))
                .transpose()?,
        })
    }
}

impl ToDatabase for TempLayout {
    type Output = TempLayoutValue;

    fn to_db(&self, field: &str) -> Result<Self::Output, CoreError> {
        match self {
            TempLayout::Single => Ok(TempLayoutValue {
                kind: "single".to_string(),
                total_size: None,
            }),
            TempLayout::Multipart(multipart) => Ok(TempLayoutValue {
                kind: "multipart".to_string(),
                total_size: Some(multipart.total_size.to_db(field)?),
            }),
        }
    }
}

impl FromDatabase<(TempLayoutValue, Vec<MultipartPart>)> for TempLayout {
    fn from_db(
        value: (TempLayoutValue, Vec<MultipartPart>),
        field: &str,
    ) -> Result<Self, CoreError> {
        let (layout, parts) = value;
        match layout.kind.as_str() {
            "single" => Ok(TempLayout::Single),
            "multipart" => {
                let total_size = layout
                    .total_size
                    .ok_or_else(|| CoreError::State(format!("missing total size for {field}")))?;
                Ok(TempLayout::Multipart(MultipartState {
                    total_size: u64::from_db(total_size, field)?,
                    parts,
                }))
            }
            _ => Err(CoreError::State(format!(
                "invalid temp layout kind for {field}: {}",
                layout.kind
            ))),
        }
    }
}
