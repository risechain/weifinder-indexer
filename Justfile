set dotenv-required := true
set dotenv-load := true

dev:
    docker-compose up -d
    cargo run

dev-headless:
    docker-compose up -d
    cargo run -- --headless
