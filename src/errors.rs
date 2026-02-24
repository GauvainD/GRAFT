use crate::compute::LogInfo;
use rayon;
use std::any::Any;
use std::io;
use std::sync::mpsc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransProofError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Send(#[from] mpsc::SendError<LogInfo>),
    #[error("Data handling thread panicked.")]
    Thread(Box<dyn Any + Send>),
    #[error(transparent)]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    #[error("Unknown transformation: {0}.")]
    UnknownTransformation(String),
}
