// @generated automatically by Diesel CLI.

diesel::table! {
    checkpoints (chain_id) {
        chain_id -> Integer,
        block_number -> Nullable<Integer>,
        block_hash -> Nullable<Text>,
    }
}
