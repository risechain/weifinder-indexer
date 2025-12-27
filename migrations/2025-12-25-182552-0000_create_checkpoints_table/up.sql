-- Your SQL goes here
CREATE TABLE checkpoints (
  chain_id INTEGER NOT NULL PRIMARY KEY,
  block_number INTEGER NOT NULL,
  block_hash TEXT NOT NULL
)
