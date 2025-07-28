use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxiaError {
    #[error("unexpected connection status: {0}")]
    Connection(String),
}
