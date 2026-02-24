use crate::constants::{GEN_TIME, MINHASH, NEO4J_TIME, NUM_BEST, SIM_TIME};
use crate::errors::*;
use crate::graph_transformation::GraphTransformation;
use crate::neo4j::write_graph_transformation;
use crate::property_graph::PropertyGraph;
use crate::similarity::{jaccard_index, property_graph_minhash};
use crate::transformation::*;
use log::info;
use probminhash::jaccard::compute_probminhash_jaccard;
use rayon::prelude::*;
use std::collections::{BinaryHeap, HashSet};
use std::convert::From;
use std::fmt::{Debug, Display};
use std::fs::OpenOptions;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{stdout, BufWriter, Write};
use std::sync::mpsc::{Receiver, SendError, Sender, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use self::souffle::{create_program_instance, Program};
const EPS: f64 = 1e-12;
pub struct SimGraph(f64, i64, GraphTransformation);

impl PartialEq for SimGraph {
    fn eq(&self, other: &Self) -> bool {
        (self.0 - other.0).abs() < EPS && self.1 == other.1
    }
}

impl PartialOrd for SimGraph {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Eq for SimGraph {}

impl Ord for SimGraph {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0 - other.0 {
            x if x < -EPS => std::cmp::Ordering::Greater,
            x if x > EPS => std::cmp::Ordering::Less,
            _ => self.1.cmp(&other.1),
        }
    }
}

impl Debug for SimGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.2, f)
    }
}

impl Display for SimGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.2, f)
    }
}

/// Should apply a set of transformations, filter the graphs and return the result
pub fn handle_graph(
    program: Program,
    g: PropertyGraph,
    t: &mut SenderVariant<LogInfo>,
    target_graph: &Option<PropertyGraph>,
) -> Result<(), TransProofError> {
    let mut start = Instant::now();
    let r = transform_graph(program, &g, target_graph);
    let num_bests = NUM_BEST.get().unwrap();
    let mut bests = BinaryHeap::with_capacity(num_bests + 1);
    let mut stored = HashSet::with_capacity(num_bests + 1);
    if let Some(generator) = r {
        for h in generator {
            {
                *GEN_TIME.lock().unwrap() += start.elapsed();
            }
            let mut hash = DefaultHasher::new();
            h.result.hash(&mut hash);
            let key: i64 = hash.finish() as i64;
            if let Some(target) = target_graph.as_ref() {
                if !stored.contains(&key) {
                    stored.insert(key);
                    start = Instant::now();
                    let sim = if let Some(sample) = *MINHASH.get().unwrap() {
                        let target_hash = target_graph
                            .as_ref()
                            .map(|g| property_graph_minhash(&g, sample))
                            .unwrap();
                        let g_hash = property_graph_minhash(&h.result, sample);
                        compute_probminhash_jaccard(&target_hash, &g_hash)
                    } else {
                        jaccard_index(&h.result, target)
                    };
                    {
                        *SIM_TIME.lock().unwrap() += start.elapsed();
                    }
                    bests.push(SimGraph(sim, key, h));
                    if bests.len() > *num_bests {
                        let removed = bests.pop().unwrap();
                        stored.remove(&removed.1);
                    }
                }
            } else {
                t.send(LogInfo::Transfo(h, "".to_string()))?;
            }
            start = Instant::now();
        }
    }
    for transfo in bests {
        t.send(LogInfo::TransfoSim(transfo, "".to_string()))?;
    }
    Ok(())
}

/// Should apply a set of transformations, filter the graphs and return the result
pub fn handle_graphs(
    program_name: &str,
    v: Vec<PropertyGraph>,
    t: SenderVariant<LogInfo>,
    target_graph: Option<PropertyGraph>,
) -> Result<(), TransProofError> {
    let init = || {
        let t = t.clone();
        let prog = create_program_instance(program_name);
        (t, prog)
    };
    v.into_par_iter().try_for_each_init(init, |mut s, x| {
        handle_graph(s.1, x, &mut s.0, &target_graph)
    })?;
    Ok(())
}

#[derive(Debug)]
pub enum LogInfo {
    Transfo(GraphTransformation, String),
    TransfoSim(SimGraph, String),
}

fn store_property_graph(g: &PropertyGraph, db: &neo4rs::Graph, rt: &tokio::runtime::Runtime) {
    let tx = rt.block_on(db.start_txn()).unwrap();
}

pub fn output_neo4j(
    receiver: Receiver<LogInfo>,
    first_run: bool,
) -> Result<(Option<f64>, Option<i64>), TransProofError> {
    //TODO remove the unwraps
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let neograph = runtime
        .block_on(neo4rs::Graph::new("localhost:7687", "", ""))
        .unwrap();
    let mut best_key = None;
    let mut best_sim = None;
    let start = Instant::now();
    let mut neo4j_time = Duration::new(0, 0);
    let mut i = 0;
    for log in receiver.iter() {
        match log {
            LogInfo::Transfo(t, _) => {
                i += 1;
                runtime.block_on(write_graph_transformation(&t, first_run, None, &neograph));
            }
            LogInfo::TransfoSim(t, _) => {
                i += 1;
                let neotime = Instant::now();
                runtime.block_on(write_graph_transformation(
                    &t.2,
                    first_run,
                    Some(t.0),
                    &neograph,
                ));
                neo4j_time += neotime.elapsed();
                {
                    *NEO4J_TIME.lock().unwrap() += neotime.elapsed();
                }
                if best_sim.map(|bsim| bsim < t.0).unwrap_or(true) {
                    info!("New best: {}", t.0);
                    info!("Best key: {}", t.1);
                    best_sim = Some(t.0);
                    best_key = Some(t.1);
                }
                // bufout.write_all(&format!("{}", t).into_bytes())?;
                // bufout.write_all(&s.into_bytes())?;
                // bufout.write_all(&['\n' as u8])?;
            }
        }
    }
    let duration = start.elapsed();
    info!("Done : {} transformation", i);
    let secs = duration.as_secs() as usize;
    let millis = (duration.subsec_nanos() as usize) / (1e6 as usize);
    info!("Took {} seconds and {} milliseconds", secs, millis,);
    info!(
        "Neo4j took {} seconds and {} milliseconds",
        neo4j_time.as_secs(),
        neo4j_time.subsec_millis()
    );
    Ok((best_sim, best_key))
}

pub fn output(
    receiver: Receiver<LogInfo>,
    filename: String,
    buffer: usize,
    append: bool,
) -> Result<(Option<f64>, Option<i64>), TransProofError> {
    let mut bufout: Box<dyn Write> = match filename.as_str() {
        "-" => Box::new(BufWriter::with_capacity(buffer, stdout())),
        _ => Box::new(BufWriter::with_capacity(
            buffer,
            OpenOptions::new()
                .write(true)
                .append(append)
                .create(true)
                .open(filename)?,
        )),
    };
    let mut best_key = None;
    let mut best_sim = None;
    let start = Instant::now();
    let mut i = 0;
    for log in receiver.iter() {
        match log {
            LogInfo::Transfo(t, s) => {
                i += 1;
                bufout.write_all(&format!("{}", t).into_bytes())?;
                bufout.write_all(&s.into_bytes())?;
                bufout.write_all(&['\n' as u8])?;
            }
            LogInfo::TransfoSim(t, s) => {
                i += 1;
                bufout.write_all(&format!("{}", t).into_bytes())?;
                bufout.write_all(&s.into_bytes())?;
                bufout.write_all(&['\n' as u8])?;
                if best_sim.map(|bsim| bsim < t.0).unwrap_or(true) {
                    best_sim = Some(t.0);
                    best_key = Some(t.1);
                }
            }
        }
    }
    let duration = start.elapsed();
    info!("Done : {} transformations", i);
    let secs = duration.as_secs() as usize;
    let millis = (duration.subsec_nanos() as usize) / (1e6 as usize);
    info!("Took {} second and {} millisecond", secs, millis,);
    Ok((best_sim, best_key))
}

//#[derive(Clone)]
pub enum SenderVariant<T>
where
    T: Send,
{
    UnlimitedSender(Sender<T>),
    LimitedSender(SyncSender<T>),
}

impl<T> SenderVariant<T>
where
    T: Send,
{
    fn send(&self, t: T) -> Result<(), SendError<T>> {
        match self {
            SenderVariant::UnlimitedSender(s) => s.send(t),
            SenderVariant::LimitedSender(s) => s.send(t),
        }
    }
}

impl<T> From<Sender<T>> for SenderVariant<T>
where
    T: Send,
{
    fn from(sender: Sender<T>) -> Self {
        SenderVariant::UnlimitedSender(sender)
    }
}

impl<T> From<SyncSender<T>> for SenderVariant<T>
where
    T: Send,
{
    fn from(sender: SyncSender<T>) -> Self {
        SenderVariant::LimitedSender(sender)
    }
}

impl<T> Clone for SenderVariant<T>
where
    T: Send,
{
    fn clone(&self) -> Self {
        match self {
            SenderVariant::UnlimitedSender(s) => SenderVariant::UnlimitedSender(s.clone()),
            SenderVariant::LimitedSender(s) => SenderVariant::LimitedSender(s.clone()),
        }
    }
}
