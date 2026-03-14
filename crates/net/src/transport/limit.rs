use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SLEEP_SLICE: Duration = Duration::from_millis(20);
const NO_OVERRIDE_KBPS: u64 = u64::MAX;

#[derive(Clone)]
pub(crate) struct SpeedLimit {
    global_kbps: Arc<AtomicU64>,
    override_kbps: Arc<AtomicU64>,
    global_state: Arc<Mutex<LimiterState>>,
    local_state: Arc<Mutex<LimiterState>>,
}

impl SpeedLimit {
    pub(crate) fn new(global_kbps: Arc<AtomicU64>, override_kbps: Option<u64>) -> Self {
        Self {
            global_kbps,
            override_kbps: speed_limit_override(override_kbps),
            global_state: Arc::new(Mutex::new(LimiterState::new())),
            local_state: Arc::new(Mutex::new(LimiterState::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn shared_global(global_kbps: u64) -> Self {
        Self::new(Arc::new(AtomicU64::new(global_kbps)), None)
    }

    pub(crate) fn for_task(&self, override_kbps: Arc<AtomicU64>) -> Self {
        Self {
            global_kbps: Arc::clone(&self.global_kbps),
            override_kbps,
            global_state: Arc::clone(&self.global_state),
            local_state: Arc::new(Mutex::new(LimiterState::new())),
        }
    }

    pub(crate) fn current_kbps(&self) -> u64 {
        self.override_kbps()
            .unwrap_or_else(|| self.global_kbps.load(Ordering::Relaxed))
    }

    pub(crate) fn current_bps(&self) -> Option<u64> {
        kbps_to_bps(Some(self.current_kbps()))
    }

    pub(crate) fn override_kbps(&self) -> Option<u64> {
        decode_override(self.override_kbps.load(Ordering::Relaxed))
    }

    pub(crate) fn override_bps(&self) -> Option<u64> {
        kbps_to_bps(self.override_kbps())
    }

    fn active_state(&self) -> &Arc<Mutex<LimiterState>> {
        match decode_override(self.override_kbps.load(Ordering::Relaxed)) {
            Some(_) => &self.local_state,
            None => &self.global_state,
        }
    }
}

impl std::fmt::Debug for SpeedLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpeedLimit")
            .field("global_kbps", &self.global_kbps.load(Ordering::Relaxed))
            .field(
                "override_kbps",
                &decode_override(self.override_kbps.load(Ordering::Relaxed)),
            )
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Limiter {
    speed_limit: SpeedLimit,
}

#[derive(Debug)]
struct LimiterState {
    tokens: f64,
    last_refill_at: Instant,
}

impl LimiterState {
    fn new() -> Self {
        Self {
            tokens: 0.0,
            last_refill_at: Instant::now(),
        }
    }
}

impl Limiter {
    pub(crate) fn new(speed_limit: SpeedLimit) -> Self {
        Self { speed_limit }
    }

    pub(crate) async fn wait_for_async<F>(&self, bytes: u64, should_stop: F)
    where
        F: Fn() -> bool,
    {
        while !should_stop() {
            let Some(limit_bps) = self.speed_limit.current_bps() else {
                return;
            };

            let now = Instant::now();
            let wait = {
                let Ok(mut state) = self.speed_limit.active_state().lock() else {
                    return;
                };

                refill_tokens(&mut state, now, limit_bps);
                let bytes = bytes as f64;
                if state.tokens >= bytes {
                    state.tokens -= bytes;
                    None
                } else {
                    let missing = bytes - state.tokens;
                    let wait_secs = missing / limit_bps as f64;
                    Some(Duration::from_secs_f64(wait_secs))
                }
            };

            match wait {
                Some(wait) if wait > Duration::ZERO => {
                    tokio::time::sleep(wait.min(SLEEP_SLICE)).await;
                }
                _ => return,
            }
        }
    }
}

pub(crate) fn speed_limit_override(override_kbps: Option<u64>) -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(encode_override(override_kbps)))
}

pub(crate) fn set_speed_limit_override(slot: &AtomicU64, override_kbps: Option<u64>) {
    slot.store(encode_override(override_kbps), Ordering::Relaxed);
}

fn encode_override(override_kbps: Option<u64>) -> u64 {
    override_kbps.unwrap_or(NO_OVERRIDE_KBPS)
}

fn decode_override(value: u64) -> Option<u64> {
    if value == NO_OVERRIDE_KBPS {
        None
    } else {
        Some(value)
    }
}

fn kbps_to_bps(limit_kbps: Option<u64>) -> Option<u64> {
    match limit_kbps {
        Some(0) | None => None,
        Some(kbps) => Some(kbps.saturating_mul(1024)),
    }
}

fn refill_tokens(state: &mut LimiterState, now: Instant, limit_bps: u64) {
    let elapsed = now.duration_since(state.last_refill_at).as_secs_f64();
    let capacity = limit_bps as f64;
    state.tokens = (state.tokens + elapsed * limit_bps as f64).min(capacity);
    state.last_refill_at = now;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::{Limiter, SpeedLimit};

    #[tokio::test]
    async fn zero_limit_is_unlimited() {
        let limiter = Limiter::new(SpeedLimit::new(Arc::new(AtomicU64::new(0)), None));
        let started = Instant::now();
        limiter.wait_for_async(32 * 1024, || false).await;
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn limiter_paces_transfers() {
        let limiter = Limiter::new(SpeedLimit::new(Arc::new(AtomicU64::new(32)), None));
        let started = Instant::now();
        limiter.wait_for_async(32 * 1024, || false).await;
        assert!(started.elapsed() >= Duration::from_millis(850));
    }

    #[tokio::test]
    async fn override_replaces_global_limit() {
        let limiter = Limiter::new(SpeedLimit::new(Arc::new(AtomicU64::new(128)), Some(16)));
        let started = Instant::now();
        limiter.wait_for_async(16 * 1024, || false).await;
        assert!(started.elapsed() >= Duration::from_millis(850));
    }

    #[test]
    fn override_reporting_only_exposes_explicit_limit() {
        let global_only = SpeedLimit::new(Arc::new(AtomicU64::new(128)), None);
        assert_eq!(global_only.override_bps(), None);

        let override_limit = SpeedLimit::new(Arc::new(AtomicU64::new(128)), Some(16));
        assert_eq!(override_limit.override_bps(), Some(16 * 1024));
    }

    #[tokio::test]
    async fn cloned_global_limiters_share_bandwidth() {
        let speed_limit = SpeedLimit::shared_global(32);
        let completed = Arc::new(AtomicUsize::new(0));

        let first = tokio::spawn({
            let speed_limit = speed_limit.clone();
            let completed = Arc::clone(&completed);
            async move {
                let limiter = Limiter::new(speed_limit);
                limiter.wait_for_async(16 * 1024, || false).await;
                completed.fetch_add(1, Ordering::SeqCst);
            }
        });
        let second = tokio::spawn({
            let speed_limit = speed_limit.clone();
            let completed = Arc::clone(&completed);
            async move {
                let limiter = Limiter::new(speed_limit);
                limiter.wait_for_async(16 * 1024, || false).await;
                completed.fetch_add(1, Ordering::SeqCst);
            }
        });

        let started = Instant::now();
        first
            .await
            .unwrap_or_else(|error| panic!("first limiter task should join: {error}"));
        second
            .await
            .unwrap_or_else(|error| panic!("second limiter task should join: {error}"));

        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= Duration::from_millis(850));
    }
}
