use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

pub static NUM_BEST: OnceLock<usize> = OnceLock::new();
pub static MINHASH: OnceLock<Option<usize>> = OnceLock::new();
pub static PATH_WEIGHT: OnceLock<f64> = OnceLock::new();
pub static IDEMPOTENCE: OnceLock<bool> = OnceLock::new();

lazy_static! {
    pub static ref TOTAL_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    pub static ref SOUFFLE_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    pub static ref NEO4J_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    pub static ref SIM_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    pub static ref GEN_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    pub static ref AUTOMATON_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    pub static ref NUM_DUP: Arc<Mutex<i64>> = Arc::new(Mutex::new(0i64));
    pub static ref NUM_TOT: Arc<Mutex<i64>> = Arc::new(Mutex::new(0i64));
}
