#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Join error")]
    Join(#[from] tokio::task::JoinError),
}
