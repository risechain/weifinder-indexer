use std::sync::{Arc, Mutex, atomic::AtomicU64};

use metrics::{HistogramFn, Recorder};
use metrics_util::{
    registry::{Registry, Storage},
    storage::Summary,
};

use crate::tui::{Stats, Tui};

struct AtomicStorageWithSummary;

impl<K> Storage<K> for AtomicStorageWithSummary {
    type Counter = Arc<AtomicU64>;

    type Gauge = Arc<AtomicU64>;

    type Histogram = SummaryWrapper;

    fn counter(&self, _: &K) -> Self::Counter {
        Arc::new(AtomicU64::new(0))
    }

    fn gauge(&self, _: &K) -> Self::Gauge {
        Arc::new(AtomicU64::new(0))
    }

    fn histogram(&self, _: &K) -> Self::Histogram {
        SummaryWrapper::new()
    }
}

#[derive(Clone)]
struct SummaryWrapper {
    summary: Arc<Mutex<Summary>>,
}

impl SummaryWrapper {
    fn new() -> Self {
        Self {
            summary: Arc::new(Mutex::new(Summary::with_defaults())),
        }
    }
}

impl HistogramFn for SummaryWrapper {
    fn record(&self, value: f64) {
        self.summary.lock().unwrap().add(value);
    }
}

pub struct TuiExporter {
    registry: Registry<metrics::Key, AtomicStorageWithSummary>,
    stats: Arc<Mutex<Stats>>,
}

impl Default for TuiExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl TuiExporter {
    pub fn new() -> Self {
        let stats = Arc::new(Mutex::new(Stats::default()));
        Tui::spawn(stats.clone());

        Self {
            registry: Registry::new(AtomicStorageWithSummary),
            stats,
        }
    }
}

impl Recorder for TuiExporter {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let inner_counter = self
            .registry
            .get_or_create_counter(key, |counter| counter.clone());

        match key.name() {
            "indexer_last_fetched_block" => {
                self.stats
                    .lock()
                    .unwrap()
                    .set_last_fetched_block(inner_counter.clone());
            }
            "indexer_last_saved_block" => {
                self.stats
                    .lock()
                    .unwrap()
                    .set_last_saved_block(inner_counter.clone());
            }
            "indexer_current_head_number" => {
                self.stats
                    .lock()
                    .unwrap()
                    .set_current_head_number(inner_counter.clone());
            }
            "indexer_reorgs_detected_total" => {
                self.stats
                    .lock()
                    .unwrap()
                    .set_reorgs_detected_total(inner_counter.clone());
            }
            _ => {}
        }

        metrics::Counter::from_arc(inner_counter)
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        let inner_gauge = self
            .registry
            .get_or_create_gauge(key, |gauge| gauge.clone());
        metrics::Gauge::from_arc(inner_gauge)
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        let inner_histogram = self
            .registry
            .get_or_create_histogram(key, |histogram| Arc::new(histogram.clone()));

        if key.name() == "indexer_block_fetch_duration_seconds" {
            self.stats
                .lock()
                .unwrap()
                .set_block_fetch_duration(inner_histogram.summary.clone());
        }

        metrics::Histogram::from_arc(inner_histogram)
    }
}
