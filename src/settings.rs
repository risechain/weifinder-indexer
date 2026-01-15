use std::num::{NonZeroU32, NonZeroUsize};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    pub rpc_ws: String,
    pub fetcher_max_blocks_per_second: NonZeroU32,
    pub batch_save_size: NonZeroUsize,
    pub catalog_db_url: String,
    pub s3_endpoint: String,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_bucket: String,
    pub checkpoint_db_path: String,
}
