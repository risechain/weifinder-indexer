# todos

- [x] Place duckdb appender in separate thread
- [x] Figure out how to synchronize appender flushing with checkpointing
- [x] Load settings from env and config file
- [x] Add --headless cli option to skip tui
- [x] Attach to remote duckdb and object storage
- [x] Add more tables to duckdb
- [x] use insert with data inlining instead when at the tip of the chain
- [x] Create docker file
- [x] Better error handling
- [x] Handle storage-level reorg
- [x] Deploy to fly.io
- [x] Fix checkpointing for different tables i.e blocks, transactions and logs all save independently but checkpoint is only take from the blocks table. We need more granular checkpoints for each table
- [x] Add block_number to more tables
- [x] Fix incorrect streaming calculation
