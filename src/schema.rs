// @generated automatically by Diesel CLI.

diesel::table! {
    checkpoints (chain_id) {
        chain_id -> Nullable<Integer>,
        block_number -> Integer,
        block_hash -> Text,
        last_updated_at -> Timestamp,
    }
}
