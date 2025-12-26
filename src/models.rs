use alloy::primitives::FixedBytes;
use diesel::prelude::*;

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::checkpoints)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Checkpoint {
    pub block_number: Option<i32>,
    pub block_hash: Option<String>,
}
