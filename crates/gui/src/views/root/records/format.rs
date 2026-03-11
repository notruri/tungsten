use tungsten_net::model::DownloadRecord;

pub(super) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

    if bytes < 1024 {
        return format!("{bytes} {}", UNITS[0]);
    }

    let mut value = bytes as f64;
    let mut unit_ix = 0usize;
    while value >= 1024.0 && unit_ix < UNITS.len() - 1 {
        value /= 1024.0;
        unit_ix += 1;
    }

    let mut text = format!("{value:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }

    format!("{text} {}", UNITS[unit_ix])
}

pub(super) fn file_name_for_sort(record: &DownloadRecord) -> String {
    record
        .destination
        .as_ref()
        .and_then(|path| path.file_name())
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
}

pub(super) fn file_name_for_display(record: &DownloadRecord) -> String {
    record
        .destination
        .as_ref()
        .and_then(|path| path.file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "resolving".to_string())
}

pub(super) fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let keep = max_chars.saturating_sub(1);
    let mut result = String::new();
    for ch in text.chars().take(keep) {
        result.push(ch);
    }
    result.push('…');
    result
}

pub(super) fn format_eta(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if hours > 0 {
        return format!("{hours}h {minutes:02}m");
    }

    if minutes > 0 {
        return format!("{minutes}m {secs:02}s");
    }

    format!("{secs}s")
}

pub(super) fn format_percentage(downloaded: u64, total: Option<u64>) -> String {
    let Some(total) = total else {
        return "-".to_string();
    };
    if total == 0 {
        return "-".to_string();
    }

    let percentage = ((downloaded as f64 / total as f64) * 100.0).clamp(0.0, 100.0);
    let mut text = format!("{percentage:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    format!("{text}%")
}
