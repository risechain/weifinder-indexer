use std::{
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Instant,
};

use metrics_util::storage::Summary;

/// A point-in-time snapshot of all stats for rendering
#[derive(Clone, Debug)]
pub struct StatsSnapshot {
    pub last_saved_block: u64,
    pub last_fetched_block: u64,
    pub current_head_number: u64,
    pub reorgs_detected_total: u64,
    pub rpc_connection_healthy: bool,
    pub uptime_seconds: u64,
    pub fetch_rps: f64,
    pub sync_eta: Option<u64>,
    pub block_fetch_p99_ms: Option<f64>,
}

/// Cached rate calculation state
struct RateState {
    last_sample_time: Instant,
    prev_fetched_block: u64,
    cached_fetch_rps: f64,
}

#[derive(Clone)]
pub struct Stats {
    // Base metrics
    pub last_saved_block: Arc<AtomicU64>,
    pub last_fetched_block: Arc<AtomicU64>,
    pub current_head_number: Arc<AtomicU64>,
    pub reorgs_detected_total: Arc<AtomicU64>,
    pub start_time: Instant,
    pub block_fetch_duration: Option<Arc<Mutex<Summary>>>,

    // For derived stats (rate calculation with caching)
    rate_state: Arc<Mutex<RateState>>,
}

impl Default for Stats {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            last_saved_block: Arc::new(AtomicU64::new(0)),
            last_fetched_block: Arc::new(AtomicU64::new(0)),
            current_head_number: Arc::new(AtomicU64::new(0)),
            reorgs_detected_total: Arc::new(AtomicU64::new(0)),
            start_time: now,
            block_fetch_duration: None,
            rate_state: Arc::new(Mutex::new(RateState {
                last_sample_time: now,
                prev_fetched_block: 0,
                cached_fetch_rps: 0.0,
            })),
        }
    }
}

impl Stats {
    pub fn set_last_saved_block(&mut self, last_saved_block: Arc<AtomicU64>) {
        self.last_saved_block = last_saved_block;
    }

    pub fn set_last_fetched_block(&mut self, last_fetched_block: Arc<AtomicU64>) {
        self.last_fetched_block = last_fetched_block;
    }

    pub fn set_current_head_number(&mut self, current_head_number: Arc<AtomicU64>) {
        self.current_head_number = current_head_number;
    }

    pub fn set_reorgs_detected_total(&mut self, reorgs_detected_total: Arc<AtomicU64>) {
        self.reorgs_detected_total = reorgs_detected_total;
    }

    pub fn set_block_fetch_duration(&mut self, summary: Arc<Mutex<Summary>>) {
        self.block_fetch_duration = Some(summary);
    }

    /// Returns uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Create a snapshot of all stats for rendering
    pub fn snapshot(&self) -> StatsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;

        let current_saved = self.last_saved_block.load(Relaxed);
        let current_fetched = self.last_fetched_block.load(Relaxed);
        let head = self.current_head_number.load(Relaxed);

        // Calculate fetch rate with caching
        let fetch_rps = {
            let mut rate_state = self.rate_state.lock().unwrap();
            let elapsed = rate_state.last_sample_time.elapsed().as_secs_f64();

            // Only recalculate if at least 5 seconds has passed
            if elapsed >= 3.0 {
                let prev_fetched_block = if rate_state.prev_fetched_block == 0 {
                    current_fetched
                } else {
                    rate_state.prev_fetched_block
                };
                let fetch_delta = current_fetched.saturating_sub(prev_fetched_block);
                rate_state.cached_fetch_rps = fetch_delta as f64 / elapsed;
                rate_state.prev_fetched_block = current_fetched;
                rate_state.last_sample_time = Instant::now();
            }

            rate_state.cached_fetch_rps
        };

        // Calculate ETA based on fetch rate
        let blocks_remaining = head.saturating_sub(current_fetched);
        let sync_eta = if fetch_rps > 0.0 && blocks_remaining > 0 {
            Some((blocks_remaining as f64 / fetch_rps) as u64)
        } else if blocks_remaining == 0 {
            None // Actually synced
        } else {
            Some(u64::MAX) // Not synced but no rate data yet
        };

        // Get p99 latency from histogram (convert seconds to milliseconds)
        let block_fetch_p99_ms = self
            .block_fetch_duration
            .as_ref()
            .and_then(|s| s.lock().ok())
            .and_then(|s| s.quantile(0.99))
            .map(|secs| secs * 1000.0);

        StatsSnapshot {
            last_saved_block: current_saved,
            last_fetched_block: current_fetched,
            current_head_number: head,
            reorgs_detected_total: self.reorgs_detected_total.load(Relaxed),
            rpc_connection_healthy: fetch_rps > 0.0,
            uptime_seconds: self.uptime_seconds(),
            fetch_rps,
            sync_eta,
            block_fetch_p99_ms,
        }
    }
}
