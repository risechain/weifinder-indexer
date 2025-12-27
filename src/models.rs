use crate::schema::checkpoints;
use diesel::prelude::*;

#[derive(Queryable, Selectable)]
#[diesel(table_name = checkpoints)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Checkpoint {
    pub block_number: i32,
    pub block_hash: String,
}

#[derive(Insertable)]
#[diesel(table_name = checkpoints)]
pub struct NewCheckpoint {
    pub chain_id: i32,
    pub block_number: i32,
    pub block_hash: String,
}
