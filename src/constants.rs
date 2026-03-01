//! Constants and accumulators used throughout the code.

use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

/// Number of candidate schemas to keep
pub static NUM_BEST: OnceLock<usize> = OnceLock::new();
/// If minhash is used, size of the sample
pub static MINHASH: OnceLock<Option<usize>> = OnceLock::new();
/// If weight_distance is used, weight of the distance vs similarity
pub static PATH_WEIGHT: OnceLock<f64> = OnceLock::new();
/// Are edit operations idempotent?
pub static IDEMPOTENCE: OnceLock<bool> = OnceLock::new();

// These are accumulators for timings and measurments
lazy_static! {
    /// Total runtime
    pub static ref TOTAL_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    /// Total time spent by souffle evaluating transformations
    pub static ref SOUFFLE_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    /// Total time spent talking to Neo4j
    pub static ref NEO4J_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    /// Total time spent computing similarity
    pub static ref SIM_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    /// Total time spent generating transformations from the automaton
    pub static ref GEN_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    /// Total time spent producing the automaton and compressing cliques
    pub static ref AUTOMATON_TIME: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::default()));
    /// Total number of schemas produced that were already in the meta-graph
    pub static ref NUM_DUP: Arc<Mutex<i64>> = Arc::new(Mutex::new(0i64));
    /// Total number of produced schemas (duplicates included)
    pub static ref NUM_TOT: Arc<Mutex<i64>> = Arc::new(Mutex::new(0i64));
}
