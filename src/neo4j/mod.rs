//! Neo4j interface for storing and querying property graph schemas.
//!
//! # Graph Layout in Neo4j
//!
//! Each property graph schema is represented by a **meta-node** (label `"Meta"`) that
//! holds a content-hash `key` property. The schema's vertices and edges live as ordinary
//! Neo4j nodes/relationships, each connected back to their meta-node via `"Inner"` edges.
//!
//! Transformation steps between schemas are stored as `"Meta"` edges between meta-nodes,
//! each carrying an `operations` list.  Once the best transformation path has been
//! identified, a `"Path"` edge is created directly from the source meta-node to the best
//! target meta-node, with the concatenated operation list.
//!
//! # Node Labels Used as State Flags
//!
//! | Label      | Meaning                                                    |
//! |------------|------------------------------------------------------------|
//! | `"Meta"`   | Meta-node representing one schema                          |
//! | `"Inner"`  | Edge from meta-node to a schema node                  |
//! | `"New"`    | Schema produced by a transformation (not yet selected)     |
//! | `"Source"` | Schema that was the starting point for transformations     |
//! | `"Target"` | Schema most similar to the desired target                  |
//! | `"Path"`   | Final shortcut edge from source to best target             |

use std::io::Write;
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    io::BufWriter,
};

use neo4rs::{query, Graph, Node, Path, Query, Relation, Txn};

use crate::constants::{
    AUTOMATON_TIME, GEN_TIME, NEO4J_TIME, NUM_DUP, NUM_TOT, PATH_WEIGHT, SIM_TIME, SOUFFLE_TIME,
    TOTAL_TIME,
};
use crate::{
    graph_transformation::GraphTransformation,
    property_graph::{Properties, PropertyGraph},
};

/// Label for schema edges without label (placeholder)
const INTERNAL_LABEL: &str = "Internal";
/// Label for meta nodes
const META_LABEL: &str = "Meta";
/// Label for edges between meta-nodes and schema nodes
const INNER_LABEL: &str = "Inner";
/// Label applied to every schema node produced by a transformation step.
///
/// Used to identify schemas that have been generated but not yet transformed.
pub const NEW_LABEL: &str = "New";
/// Label applied to the original source schema meta-node.
pub const SOURCE_LABEL: &str = "Source";
/// Label applied to the meta-node that is most similar to the desired target schema.
pub const TARGET_LABEL: &str = "Target";
/// Label for the direct shortcut edge written from source to best target.
const PATH_LABEL: &str = "Path";
/// Connection parameters for a Neo4j instance.
#[derive(Clone, Debug)]
pub struct Neo4jConfig {
    pub uri: String,
    pub user: String,
    pub password: String,
}

impl Neo4jConfig {
    pub fn new(uri: impl Into<String>, user: impl Into<String>, password: impl Into<String>) -> Self {
        Neo4jConfig { uri: uri.into(), user: user.into(), password: password.into() }
    }
}

/// Boolean-valued property set to `true` on the creation timestamp of a meta-node.
///
/// Used by [`get_or_create_metanode`] to detect whether the node was just created
/// (`true`) or already existed (`false`).
const CREATED_PROP: &str = "created";
/// Property that stores the content-hash key on every meta-node.
const KEY_PROP: &str = "key";
/// Property that stores the logical name of a schema vertex or edge type.
const NAME_PROP: &str = "_name";
/// Property that stores the similarity score of a schema to the target.
const SIM_PROP: &str = "similarity";
/// Property that stores the shortest transformation distance from a source schema.
const DISTANCE_PROP: &str = "distance";
/// Property that stores the identifier of the transformation rule that produced an edge.
const TRANSFO_ID_PROP: &str = "transfo_id";
/// Property name used on `"Meta"` edges and `"Path"` edges to carry the list of
/// applied operation strings.
pub const OPERATIONS_PROP: &str = "operations";

/// Atomically creates or retrieves the meta-node for a schema identified by `key`.
///
/// On first creation the node receives the `"Meta"` label plus optional `"New"` / `"Source"`
/// labels and an optional similarity score.  On every call the distance property is updated to the
/// minimum of the stored value and the supplied `distance` (if any).
///
/// Increments the global [`NUM_TOT`] counter, and [`NUM_DUP`] when the node already
/// existed.
///
/// # Returns
/// `(created, distance)` where `created` is `true` iff the node was just inserted
/// and `distance` is the (possibly updated) stored distance value.
async fn get_or_create_metanode(
    key: i64,
    is_output: bool,
    is_source: bool,
    sim: Option<f64>,
    distance: Option<i64>,
    conn: &mut Txn,
) -> (bool, Option<i64>) {
    let add_new = if is_output {
        format!(", n:{new}", new = NEW_LABEL)
    } else {
        "".to_string()
    };
    let add_source = if is_source {
        format!(", n:{source}", source = SOURCE_LABEL)
    } else {
        "".to_string()
    };
    let remove_new = "".to_string();
    let set_sim = sim
        .map(|s| format!(", n.{}={}", SIM_PROP, s))
        .unwrap_or("".to_string());
    let set_distance = distance
        .map(|d| {
            format!(
                "
with case
    when n.{distance} is null then {d}
    when n.{distance} > {d} then {d}
    else n.{distance}
end as distance, created, n
set n.{distance} = distance
",
                distance = DISTANCE_PROP,
                d = d
            )
        })
        .unwrap_or("".to_string());
    let query = query(&format!(
        "
call {{
with timestamp() as time
merge (n:{meta} {{{key}:$key}})
on create
set n.{created} = time {set_sim} {add_new} {add_source}
return n,n.{created} = time as created
}}
{remove_new}
{set_distance}
return created, n.{distance} as distance;
",
        add_new = add_new,
        add_source = add_source,
        set_sim = set_sim,
        remove_new = remove_new,
        key = KEY_PROP,
        created = CREATED_PROP,
        meta = META_LABEL,
        distance = DISTANCE_PROP
    ))
    .param("key", key);
    let mut data = conn.execute(query).await.unwrap();
    let row = data.next(conn.handle()).await.unwrap().unwrap();
    let created: bool = row.get("created").unwrap();
    *NUM_TOT.lock().unwrap() += 1;
    if !created {
        *NUM_DUP.lock().unwrap() += 1;
    }
    let distance = row.get("distance").ok();
    (created, distance)
}

/// Writes a single node or edge pattern fragment into a Cypher `CREATE` clause.
///
/// # Parameters
/// - `out`     – destination buffer
/// - `labels`  – ordered list of label/type strings
/// - `props`   – property map plus the logical `_name` of the element
/// - `edge`    – `true` for a relationship pattern, `false` for a node pattern
/// - `varname` – Cypher variable name to assign (e.g. `"node_0"`, `"edge_3"`)
fn format_data(
    out: &mut BufWriter<Vec<u8>>,
    labels: &Vec<&String>,
    props: &Properties,
    edge: bool,
    varname: String,
) {
    write!(out, "{}", varname);
    if edge && labels.is_empty() {
        write!(out, ":{}", INTERNAL_LABEL);
    } else {
        let mut start = true;
        for label in labels {
            if !start && edge {
                write!(out, "_");
            } else {
                write!(out, ":");
                start = false;
            }
            write!(out, "{}", label);
        }
    }
    write!(out, " {{ {name}:\"{}\"", props.name, name = NAME_PROP);
    for (key, typ) in props.map.iter() {
        write!(out, ", ");
        write!(out, "{}:\"{}\"", key, typ);
    }
    write!(out, " }}");
}

/// Builds the Cypher query string that creates all nodes and edges of a
/// [`PropertyGraph`] under an already-existing meta-node.
///
/// The returned string is parameterised with `$key` and is intended to be used with
/// [`neo4rs::query`] by supplying the hash key as a parameter.
fn create_property_graph_query(g: &PropertyGraph) -> String {
    let mut out = BufWriter::new(Vec::new());
    write!(
        out,
        "MATCH (_meta:{meta} {{{key}:$key}}) CREATE ",
        meta = META_LABEL,
        key = KEY_PROP
    );
    let mut names = HashMap::new();
    let mut start = true;
    for (num, vertex) in g.graph.node_indices().enumerate() {
        if start {
            start = false;
        } else {
            write!(out, ", ");
        }
        let props = g.graph.node_weight(vertex).unwrap();
        names.insert(vertex, format!("node_{}", num));
        let labels = g
            .vertex_label
            .element_labels(&vertex)
            .map(|id| g.vertex_label.get_label(*id).unwrap())
            .collect();
        write!(out, "( ");
        format_data(&mut out, &labels, props, false, format!("node_{}", num));
        write!(out, " )");
    }
    for (num, edge) in g.graph.edge_indices().enumerate() {
        let (from, to) = g.graph.edge_endpoints(edge).unwrap();
        let props = g.graph.edge_weight(edge).unwrap();
        let labels = g
            .edge_label
            .element_labels(&edge)
            .map(|id| g.edge_label.get_label(*id).unwrap())
            .collect();
        write!(out, ", ({})", names.get(&from).unwrap());
        write!(out, "  -[");
        format_data(&mut out, &labels, props, true, format!("edge_{}", num));
        write!(out, " ]->");
        write!(out, "({})", names.get(&to).unwrap());
    }
    for name in names.values() {
        write!(
            out,
            ", (_meta)-[:{inner}]->({name})",
            inner = INNER_LABEL,
            name = name
        );
    }
    write!(out, ";");
    let res = String::from_utf8(out.into_inner().unwrap()).unwrap();
    res
}

/// Persists a [`PropertyGraph`] to Neo4j, creating its meta-node if absent.
///
/// Computes a [`DefaultHasher`] hash of the graph to use as the unique `key`.
/// Opens a transaction, calls [`get_or_create_metanode`], and if the meta-node is
/// new, runs the full node/edge creation query.
///
/// # Parameters
/// - `g`          – the graph to persist
/// - `is_output`  – if `true`, the meta-node receives the `"New"` label
/// - `is_source`  – if `true`, the meta-node receives the `"Source"` label
/// - `sim`        – optional similarity score to store on the meta-node
/// - `distance`   – optional transformation distance from the source to store
/// - `conn`       – Neo4j connection pool
///
/// # Returns
/// `(key, distance)` — the hash key of the graph and its stored distance value.
async fn write_property_graph(
    g: &PropertyGraph,
    is_output: bool,
    is_source: bool,
    sim: Option<f64>,
    distance: Option<i64>,
    conn: &Graph,
) -> (i64, Option<i64>) {
    let mut hash = DefaultHasher::new();
    g.hash(&mut hash);
    let key = hash.finish() as i64;
    let mut tx = conn.start_txn().await.unwrap();
    let (exists, distance) =
        get_or_create_metanode(key, is_output, is_source, sim, distance, &mut tx).await;
    if exists {
        let query = query(&create_property_graph_query(g)).param("key", key);
        tx.run(query).await.unwrap();
    }
    tx.commit().await.unwrap();
    (key, distance)
}

/// Builds the Cypher query string that creates a `"Meta"` edge between two meta-nodes.
///
/// The edge carries an `operations` list parameter and an optional `transfo_id`
/// property identifying the transformation rule that generated the step.
///
/// The returned string is parameterised with `$first_key`, `$second_key`, and
/// `$operations`.
fn build_meta_edge_query(transfo_id: Option<String>) -> String {
    let id_param = transfo_id
        .map(|id_text| format!(", {}: \"{}\"", TRANSFO_ID_PROP, id_text))
        .unwrap_or_else(String::new);
    let start = format!(
        "
MATCH (n1: {meta} {{{key}:$first_key}}), (n2: {meta} {{{key}:$second_key}})
CREATE (n1) -[:{meta} {{{ops}:$operations{id_text}}}]-> (n2);
",
        key = KEY_PROP,
        meta = META_LABEL,
        ops = OPERATIONS_PROP,
        id_text = id_param,
    );
    start.to_string()
}

/// Writes a complete [`GraphTransformation`] (before/after pair) to Neo4j.
///
/// # Parameters
/// - `gt`        – the transformation to record
/// - `is_source` – whether `gt.init` is the original user-supplied source schema
/// - `sim`       – similarity score of the result schema to the target (if known)
/// - `conn`      – Neo4j connection pool
pub async fn write_graph_transformation(
    gt: &GraphTransformation,
    is_source: bool,
    sim: Option<f64>,
    conn: &Graph,
) {
    let first = &gt.init;
    let dist = if is_source { Some(0) } else { None };
    let (first_key, mut dist) =
        write_property_graph(first, false, is_source, None, dist, conn).await;
    let second = &gt.result;
    dist.iter_mut().for_each(|v| *v += 1);
    let (second_key, _) = write_property_graph(second, true, false, sim, dist, conn).await;
    let q = build_meta_edge_query(gt.transfo_id.clone());
    let query = query(&build_meta_edge_query(gt.transfo_id.clone()))
        .param("first_key", first_key as i64)
        .param("second_key", second_key as i64)
        .param("operations", gt.operations.clone());
    conn.run(query).await.unwrap();
}

/// Strategy for choosing which schema(s) to fetch from Neo4j as the next input.
///
/// Implementors build a Cypher query that returns rows with three columns:
/// - `key` (`i64`) — the hash key of the selected meta-node
/// - `n`   (`Vec<Node>`) — all schema vertices reachable via `[:Inner]` edges
/// - `e`   (`Vec<Relation>`) — all schema edges reachable via `[:Inner]` edges
pub trait SourceSelector {
    /// Builds the Cypher query that selects source schemas with the given Neo4j label.
    fn build_query(label: &str) -> Query;
}

/// Fetches **all** schemas that carry `label`, with no ordering or limit.
///
/// Useful when every stored schema must be processed in the next iteration.
pub struct NaiveSource;

impl SourceSelector for NaiveSource {
    fn build_query(label: &str) -> Query {
        query(&format!(
            "match (s:{selected})
return
s.{id_prop} as key,
collect {{ match (s)-[:{inner}]->(n) return n }} as n,
collect {{ match (s)-[:{inner}]->()-[e:!{inner}]->() return e }} as e;
",
            id_prop = KEY_PROP,
            selected = label,
            inner = INNER_LABEL
        ))
    }
}

/// Fetches the **single** schema with the highest stored similarity score.
///
/// Acts as a greedy best-first strategy: always continue from the schema that is
/// currently closest to the target.
pub struct GreedySource;

impl SourceSelector for GreedySource {
    fn build_query(label: &str) -> Query {
        query(&format!(
            "match (s:{selected})
return
s.{id_prop} as key,
collect {{ match (s)-[:{inner}]->(n) return n }} as n,
collect {{ match (s)-[:{inner}]->()-[e:!{inner}]->() return e }} as e
order by s.{similarity} desc
limit 1;
",
            id_prop = KEY_PROP,
            selected = label,
            inner = INNER_LABEL,
            similarity = SIM_PROP
        ))
    }
}

/// Fetches the **single** schema that minimises a weighted combination of
/// transformation distance and dissimilarity to the target.
///
/// The objective function is:
/// ```text
/// w * (distance / maxDist) + (1 - w) * (1 - similarity)
/// ```
/// where `w` is [`PATH_WEIGHT`].  Selecting the minimum balances exploration
/// (short distance from source) against exploitation (high similarity to target).
pub struct WeightedDistanceSource;

impl SourceSelector for WeightedDistanceSource {
    fn build_query(label: &str) -> Query {
        let weight = PATH_WEIGHT.get().unwrap();
        //FIXME only get the best one
        query(&format!(
            "match (n:{meta})
with max(n.{distance}) as maxDist
match (s:{selected})
return
s.{id_prop} as key,
collect {{ match (s)-[:{inner}]->(n) return n }} as n,
collect {{ match (s)-[:{inner}]->()-[e:!{inner}]->() return e }} as e
order by {weight}*(s.{distance} / maxDist) + (1 - {weight})*(1 - s.{similarity})
limit 1;
",
            id_prop = KEY_PROP,
            selected = label,
            inner = INNER_LABEL,
            similarity = SIM_PROP,
            meta = META_LABEL,
            distance = DISTANCE_PROP,
            weight = weight
        ))
    }
}

/// Fetches a **single** schema chosen uniformly at random from those with `label`.
///
/// Useful for stochastic exploration or baseline comparisons.
pub struct RandomSource;

impl SourceSelector for RandomSource {
    fn build_query(label: &str) -> Query {
        //FIXME only get the best one
        query(&format!(
            "match (s:{selected})
with s, rand() as r
return
s.{id_prop} as key,
collect {{ match (s)-[:{inner}]->(n) return n }} as n,
collect {{ match (s)-[:{inner}]->()-[e:!{inner}]->() return e }} as e
order by r
limit 1;
",
            id_prop = KEY_PROP,
            selected = label,
            inner = INNER_LABEL,
        ))
    }
}

/// Runtime-selectable wrapper over the four [`SourceSelector`] strategies.
///
/// Chosen via the `--strat` CLI flag and passed through the main iteration loop.
pub enum SourceSelectorEnum {
    /// Select a single schema at random.
    Random,
    /// Select the schema with the highest similarity score.
    Greedy,
    /// Select the schema minimising the weighted distance/dissimilarity objective.
    WeightedDistance,
    /// Return all schemas without filtering.
    Naive,
}

impl SourceSelectorEnum {
    /// Delegates to the appropriate [`SourceSelector::build_query`] implementation.
    pub fn build_query(&self, label: &str) -> Query {
        match self {
            SourceSelectorEnum::Random => RandomSource::build_query(label),
            SourceSelectorEnum::Greedy => GreedySource::build_query(label),
            SourceSelectorEnum::WeightedDistance => WeightedDistanceSource::build_query(label),
            SourceSelectorEnum::Naive => NaiveSource::build_query(label),
        }
    }
}

/// Async implementation of [`get_source_graphs`].
///
/// Executes the selector's query, then deserialises each result row back into a
/// [`PropertyGraph`]:
///
/// Returns a `Vec` of `(key, graph)` pairs.
async fn get_source_graphs_async(
    label: &str,
    conn: &Graph,
    source: &SourceSelectorEnum,
) -> Vec<(i64, PropertyGraph)> {
    let mut graphs = Vec::new();
    let query = source.build_query(label);
    let mut res = conn.execute(query).await.unwrap();
    while let Ok(Some(row)) = res.next().await {
        let mut g = PropertyGraph::default();
        let mut ids = HashMap::new();
        let nodes: Vec<Node> = row.get("n").unwrap();
        for node in nodes {
            let mut props = HashMap::new();
            let mut name = None;
            for key in node.keys() {
                if key == NAME_PROP {
                    name = Some(node.get(key).unwrap());
                } else {
                    props.insert(key.to_string(), node.get(key).unwrap());
                }
            }
            let props = Properties {
                name: name.unwrap(),
                map: props,
            };
            let id = g.graph.add_node(props);
            for label in node.labels() {
                let lid = g.vertex_label.add_label(label.to_string());
                g.vertex_label.add_label_mapping(&id, lid).unwrap();
            }
            ids.insert(node.id(), id);
        }
        let edges: Vec<Relation> = row.get("e").unwrap();
        for edge in edges {
            let mut props = HashMap::new();
            let mut name = None;
            for key in edge.keys() {
                if key == NAME_PROP {
                    name = Some(edge.get(key).unwrap());
                } else {
                    props.insert(key.to_string(), edge.get(key).unwrap());
                }
            }
            let props = Properties {
                name: name.unwrap(),
                map: props,
            };
            let from_id = ids.get(&edge.start_node_id()).unwrap();
            let to_id = ids.get(&edge.end_node_id()).unwrap();
            let id = g.graph.add_edge(*from_id, *to_id, props);
            let label = edge.typ();
            if label != INTERNAL_LABEL {
                let lid = g.edge_label.add_label(label.to_string());
                g.edge_label.add_label_mapping(&id, lid).unwrap();
            }
        }
        graphs.push((row.get("key").unwrap(), g));
    }
    graphs
}

/// Fetches property graph schemas from Neo4j and deserialises them into
/// [`PropertyGraph`] values.
///
/// This is the synchronous wrapper around [`get_source_graphs_async`]; it spins up
/// a single-threaded Tokio runtime internally.
///
/// # Returns
/// A `Vec` of `(key, graph)` where `key` is the stored hash of the schema.
pub fn get_source_graphs(label: &str, selector: &SourceSelectorEnum, config: &Neo4jConfig) -> Vec<(i64, PropertyGraph)> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let neograph = runtime
        .block_on(neo4rs::Graph::new(&config.uri, &config.user, &config.password))
        .unwrap();
    runtime.block_on(get_source_graphs_async(label, &neograph, selector))
}

/// Async implementation of [`remove_label`].
///
/// Generates an `UNWIND`-based Cypher query so that all `keys` are processed in a
/// single round-trip.
async fn remove_label_async(label: &str, keys: &[i64], conn: &Graph) {
    let query_str = format!(
        "
unwind {keys:?} as key
match (n {{{key_label}:key}})
remove n:{label};
        ",
        key_label = KEY_PROP,
        keys = keys,
        label = label
    );
    let query = query(&query_str);
    conn.run(query).await.unwrap();
}

/// Async implementation of [`add_label`].
async fn add_label_async(label: &str, key: i64, conn: &Graph) {
    let query_str = format!(
        "
match (n {{{key}:$key}})
set n:{label};
        ",
        key = KEY_PROP,
        label = label
    );
    let query = query(&query_str).param("key", key as i64);
    conn.run(query).await.unwrap();
}

/// Removes a Neo4j label from all meta-nodes whose `key` is in `keys`.
///
/// Typical use: strip the `"New"` label from schemas that have just been selected
/// for the next transformation round.
pub fn remove_label(label: &str, keys: &[i64], config: &Neo4jConfig) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let neograph = runtime
        .block_on(neo4rs::Graph::new(&config.uri, &config.user, &config.password))
        .unwrap();
    runtime.block_on(remove_label_async(label, keys, &neograph))
}

/// Adds a Neo4j label to the meta-node identified by `key`.
///
/// Typical use: mark a schema node as `"Target"` once it has been identified as
/// the best result.
pub fn add_label(label: &str, key: i64, config: &Neo4jConfig) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let neograph = runtime
        .block_on(neo4rs::Graph::new(&config.uri, &config.user, &config.password))
        .unwrap();
    runtime.block_on(add_label_async(label, key, &neograph))
}

/// Async implementation of [`compute_paths`].
///
/// For every pair `(source, target)` connected by a shortest `"Meta"`-edge path,
/// concatenates the `operations` lists from each intermediate edge into a single
/// flat `Vec<String>` and writes a direct `"Path"` edge between the two endpoint
/// meta-nodes, storing the combined operations list under `operations_name`.
async fn compute_paths_async(
    source_label: &str,
    target_label: &str,
    operations_name: &str,
    conn: &Graph,
) {
    let path_query = format!(
        "
match p=shortest 1 (s:{source})-[:{meta}]-*(t:{target})
return p;
    ",
        source = source_label,
        meta = META_LABEL,
        target = target_label
    );

    let add_edge_query = format!(
        "
match (s {{{key}:$key_source}}), (t {{{key}:$key_target}})
create (s)-[:{path} {{{ops}:$ops}}]->(t);
    ",
        key = KEY_PROP,
        ops = operations_name,
        path = PATH_LABEL
    );

    let mut paths = conn.execute(query(&path_query)).await.unwrap();
    while let Some(row) = paths.next().await.unwrap() {
        let path: Path = row.get("p").unwrap();
        let nodes = path.nodes();
        let ops: Vec<String> = path
            .rels()
            .iter()
            .flat_map(|rel| rel.get::<Vec<String>>(operations_name).unwrap().into_iter())
            .collect();
        let first_key = nodes.first().map(|n| n.get::<i64>("key").unwrap()).unwrap();
        let last_key = nodes.last().map(|n| n.get::<i64>("key").unwrap()).unwrap();
        let query = query(&add_edge_query)
            .param("key_source", first_key)
            .param("key_target", last_key)
            .param("ops", ops);
        conn.run(query).await.unwrap();
    }
}

/// Materialises shortest-path `"Path"` edges from every `source_label` node to
/// every reachable `target_label` node in the transformation graph.
///
/// For each shortest path found (traversing `"Meta"` edges), the operations from
/// every intermediate step are concatenated and stored as a single list on a new
/// direct `"Path"` edge.  The property name used for the list is `operations_name`.
pub fn compute_paths(source_label: &str, target_label: &str, operations_name: &str, config: &Neo4jConfig) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let neograph = runtime
        .block_on(neo4rs::Graph::new(&config.uri, &config.user, &config.password))
        .unwrap();
    runtime.block_on(compute_paths_async(
        source_label,
        target_label,
        operations_name,
        &neograph,
    ))
}

/// Async implementation of [`save_timings`].
///
/// Creates a single `TIMINGS` node containing all wall-clock measurements from the
/// global timing accumulators and the duplicate/total schema counters.
async fn save_timings_async(neograph: &Graph) {
    let num_dup: i64 = { *NUM_DUP.lock().unwrap() };
    let num_tot: i64 = { *NUM_TOT.lock().unwrap() };
    let query = query(
        "
CREATE (n:TIMINGS {
    total_time: $total_time,
    souffle_time: $souffle_time,
    neo4j_time: $neo4j_time,
    sim_time: $sim_time,
    gen_time: $gen_time,
    automaton_time: $automaton_time,
    num_dup: $num_dup,
    num_tot: $num_tot
});",
    )
    .param("total_time", TOTAL_TIME.lock().unwrap().as_secs_f64())
    .param("souffle_time", SOUFFLE_TIME.lock().unwrap().as_secs_f64())
    .param("neo4j_time", NEO4J_TIME.lock().unwrap().as_secs_f64())
    .param("sim_time", SIM_TIME.lock().unwrap().as_secs_f64())
    .param("gen_time", GEN_TIME.lock().unwrap().as_secs_f64())
    .param(
        "automaton_time",
        AUTOMATON_TIME.lock().unwrap().as_secs_f64(),
    )
    .param("num_dup", num_dup)
    .param("num_tot", num_tot);
    neograph.run(query).await.unwrap();
}

/// Persists a `TIMINGS` node to Neo4j with all accumulated profiling data.
///
/// Writes the following properties (all durations in seconds as `f64`):
/// - `total_time`, `souffle_time`, `neo4j_time`, `sim_time`, `gen_time`,
///   `automaton_time` — phase wall-clock totals
/// - `num_dup` — number of schemas already present when attempted to be inserted
/// - `num_tot` — total number of schema insertion attempts
pub fn save_timings(config: &Neo4jConfig) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let neograph = runtime
        .block_on(neo4rs::Graph::new(&config.uri, &config.user, &config.password))
        .unwrap();
    runtime.block_on(save_timings_async(&neograph));
}
