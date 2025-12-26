use std::num::{NonZeroU32, NonZeroUsize};

pub struct Settings {
    pub db_url: String,
    pub rpc_url: String,
    pub fetcher_max_concurrency: NonZeroUsize,
    pub fetcher_max_rps: NonZeroU32,
    pub data_path: String,
}
