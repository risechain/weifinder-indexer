create table if not exists checkpoints (
    chain_id integer primary key,
    block_number integer not null,
    block_hash text not null,
    last_updated_at timestamp not null default current_timestamp
);
