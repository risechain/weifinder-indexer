-- Your SQL goes here
CREATE TABLE checkpoints (
  chain_id INTEGER NOT NULL PRIMARY KEY,
  block_number INTEGER,
  block_hash TEXT
)
