//! Error types for the library

use crate::compute::LogInfo;
use rayon;
use std::any::Any;
use std::io;
use std::sync::mpsc;
use thiserror::Error;

/// General error type
#[derive(Error, Debug)]
pub enum TransProofError {
    /// Error from IO
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Error for inter-thread communication
    #[error(transparent)]
    Send(#[from] mpsc::SendError<LogInfo>),
    /// Error from data handling thread
    #[error("Data handling thread panicked.")]
    Thread(Box<dyn Any + Send>),
    /// Error when building thread pool
    #[error(transparent)]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    /// Error for transformations
    #[error("Unknown transformation: {0}.")]
    UnknownTransformation(String),
}
