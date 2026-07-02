//! Module dedicated to handling similarity between property graph schemas
use std::collections::HashMap;

use petgraph::graph::{EdgeIndex, NodeIndex};
use probminhash::probminhasher::ProbMinHash3aSha;

use crate::property_graph::PropertyGraph;

/// A weighted feature: `(weight, value)`. The weight is determined at construction
/// time by the feature's role (label, name, property, adjacency, inner) so downstream
/// scoring code never has to re-derive it via string-prefix matching.
pub type Feature = (f64, String);

// Atomic schema features carry more signal than the quadratic adjacency / inner
// cross-products, which would otherwise dominate the sum by sheer volume.
const W_LABEL: f64 = 4.0;
const W_NAME: f64 = 3.0;
const W_PROP: f64 = 2.0;
const W_ADJ: f64 = 1.0;
const W_PAIR: f64 = 1.0;
const W_INNER: f64 = 0.25;

/// Extracts features from a node. One feature per node type/name, property and label.
pub fn node_base_features(g: &PropertyGraph, n: &NodeIndex) -> Vec<Feature> {
    let mut features = Vec::new();
    let weight = g.graph.node_weight(*n).unwrap();
    features.push((W_NAME, format!("node:name:{}", weight.name)));
    for prop in weight.map.iter() {
        features.push((W_PROP, format!("node:prop:{}:{}", prop.0, prop.1)));
    }
    for label in g.vertex_label.element_labels(n).map(|id| g.vertex_label.get_label(*id).unwrap()) {
        features.push((W_LABEL, format!("node:label:{}", label)));
    }
    features
}

/// Extracts features from the data of an edge. One feature per edge type/name, property and label.
pub fn edge_base_features(g: &PropertyGraph, e: &EdgeIndex) -> Vec<Feature> {
    let mut features = Vec::new();
    let weight = g.graph.edge_weight(*e).unwrap();
    features.push((W_NAME, format!("edge:name:{}", weight.name)));
    for prop in weight.map.iter() {
        features.push((W_PROP, format!("edge:prop:{}:{}", prop.0, prop.1)));
    }
    for label in g.edge_label.element_labels(e).map(|id| g.edge_label.get_label(*id).unwrap()) {
        features.push((W_LABEL, format!("edge:label:{}", label)));
    }
    features
}

/// Generate features from the features of a node or edge with no regard to connectivity (only data
/// of the element). See pair_features and adj_features for other features.
pub fn inner_features(base_features: &[Feature]) -> Vec<Feature> {
    let mut res: Vec<Feature> = base_features.to_vec();
    for i in 0..base_features.len() {
        for j in 0..base_features.len() {
            if i != j {
                res.push((W_INNER, format!("inner:{};{}", base_features[i].1, base_features[j].1)));
            }
        }
    }
    res
}

/// Generate features from two sets of features. For example, a node and an incident edge.
/// The produced features all carry the given `weight`.
pub fn pair_features(first_features: &[Feature], second_features: &[Feature], prefix: &str, weight: f64) -> Vec<Feature> {
    let mut res = Vec::new();
    for f1 in first_features.iter() {
        for f2 in second_features.iter() {
            res.push((weight, format!("{}{};{}", prefix, f1.1, f2.1)));
        }
    }
    res
}

/// Generates all features from an edge and its incident nodes. Pairs are created between features
/// of the two nodes and features of the edge with each node.
pub fn adj_features(from_features: &[Feature], to_features: &[Feature], edge_features: &[Feature]) -> Vec<Feature> {
    pair_features(from_features, to_features, "adj:", W_ADJ).into_iter()
        .chain(pair_features(from_features, edge_features, "", W_PAIR))
        .chain(pair_features(edge_features, to_features, "", W_PAIR))
        .collect()
}

/// Extracts features from a property graph that include information about adjacency as well as
/// data. Each feature carries the weight assigned by its constructor.
pub fn property_graph_features(g: &PropertyGraph) -> Vec<Feature> {
    let node_features: HashMap<NodeIndex, Vec<Feature>> = g.graph.node_indices().map(|id| (id, node_base_features(g, &id))).collect();
    g.graph.node_indices().flat_map(|id| inner_features(node_features.get(&id).unwrap()).into_iter())
        .chain(g.graph.edge_indices().flat_map(|id| {
            let ef = edge_base_features(g, &id);
            let (from, to) = g.graph.edge_endpoints(id).unwrap();
            let ff = node_features.get(&from).unwrap();
            let tf = node_features.get(&to).unwrap();
            inner_features(&ef).into_iter().chain(adj_features(ff, tf, &ef))
        }))
        .collect()
}

/// Computes the minhash signature of a property graph schema with a sample of size `sample`.
/// Features are weighted with their frequency of occurrence (feature-construction weights
/// are not used by minhash).
pub fn property_graph_minhash(g: &PropertyGraph, sample: usize) -> Vec<String> {
    let features = property_graph_features(g).into_iter().fold(HashMap::new(), |mut map, (_, value)| {
        *map.entry(value).or_insert(0u64) += 1;
        map
    });
    let mut minhash = ProbMinHash3aSha::new(sample, "".to_string());
    minhash.hash_weigthed_hashmap(&features);
    minhash.get_signature().to_vec()
}

/// Frequency multiset of features for a property graph, indexed by the feature value
/// and storing the construction-time weight alongside the count. Compute once per
/// schema and reuse across repeated similarity calls (e.g. comparing many candidates
/// to a fixed target).
pub fn feature_multiset(g: &PropertyGraph) -> HashMap<String, (f64, u64)> {
    property_graph_features(g)
        .into_iter()
        .fold(HashMap::new(), |mut h, (w, value)| {
            h.entry(value).and_modify(|e| e.1 += 1).or_insert((w, 1));
            h
        })
}

/// IDF weights `ln(N / df(f))` over a corpus of feature multisets. Rare, distinguishing
/// features are amplified; features common to every schema vanish.
pub fn compute_idf<'a, I>(corpus: I) -> HashMap<String, f64>
where
    I: IntoIterator<Item = &'a HashMap<String, (f64, u64)>>,
{
    let mut df: HashMap<String, u64> = HashMap::new();
    let mut n: u64 = 0;
    for fs in corpus {
        n += 1;
        for f in fs.keys() {
            *df.entry(f.clone()).or_insert(0) += 1;
        }
    }
    let n = n as f64;
    df.into_iter()
        .map(|(f, d)| (f, (n / d as f64).ln()))
        .collect()
}

/// Generalised weighted Jaccard (Ruzicka) similarity between two feature multisets:
///
/// ```text
///     sum_f  w(f) * min(a_f, b_f)   /   sum_f  w(f) * max(a_f, b_f)
/// ```
///
/// where `a_f`, `b_f` are the (optionally length-normalised) counts of feature `f`
/// in the two schemas. `w(f)` is the construction-time weight carried by the feature
/// (same on both sides for any given key) multiplied by an optional IDF factor.
/// Passing `idf = None` skips the IDF factor; setting `normalise = true` divides
/// counts by the total feature mass of each schema to remove size bias.
pub fn weighted_jaccard_index(
    f1: &HashMap<String, (f64, u64)>,
    f2: &HashMap<String, (f64, u64)>,
    idf: Option<&HashMap<String, f64>>,
    normalise: bool,
) -> f64 {
    let (n1, n2) = if normalise {
        (
            f1.values().map(|(_, c)| *c).sum::<u64>().max(1) as f64,
            f2.values().map(|(_, c)| *c).sum::<u64>().max(1) as f64,
        )
    } else {
        (1.0, 1.0)
    };
    let mut num = 0.0;
    let mut den = 0.0;
    for f in f1.keys().chain(f2.keys().filter(|k| !f1.contains_key(*k))) {
        let (w1, a) = f1.get(f).copied().unwrap_or((0.0, 0));
        let (w2, b) = f2.get(f).copied().unwrap_or((0.0, 0));
        // w1 == w2 when both sides have the feature; otherwise pick the existing one.
        let w = w1.max(w2) * idf.and_then(|m| m.get(f).copied()).unwrap_or(1.0);
        let a = a as f64 / n1;
        let b = b as f64 / n2;
        num += w * a.min(b);
        den += w * a.max(b);
    }
    if den == 0.0 { 0.0 } else { num / den }
}

/// Convenience wrapper over [`weighted_jaccard_index`] taking two property graphs
/// directly. For repeated calls against a fixed target, prefer building the target
/// multiset once with [`feature_multiset`] and calling `weighted_jaccard_index`.
pub fn weighted_jaccard_graphs(
    g1: &PropertyGraph,
    g2: &PropertyGraph,
    idf: Option<&HashMap<String, f64>>,
    normalise: bool,
) -> f64 {
    weighted_jaccard_index(&feature_multiset(g1), &feature_multiset(g2), idf, normalise)
}

/// Computes the Jaccard index between two property graph schemas from their respective sets of
/// features. Features are weighted with their frequency of occurrence (construction-time
/// weights are not used).
pub fn jaccard_index(g1: &PropertyGraph, g2: &PropertyGraph) -> f64 {
    let mut isolated1 = property_graph_features(g1)
        .into_iter()
        .fold(HashMap::new(), |mut h, (_, s)| {
            h.entry(s).and_modify(|v| *v += 1).or_insert(1u64);
            h
        });
    let mut common: HashMap<String, u64> = HashMap::new();
    let mut isolated2: HashMap<String, u64> = HashMap::new();
    property_graph_features(g2).into_iter().for_each(|(_, s)| {
        let num1 = isolated1.get_mut(&s).map(|v| { *v -= 1; v });
        if let Some(num1) = num1 {
            if *num1 == 0u64 {
                isolated1.remove(&s);
            }
            common.entry(s).and_modify(|v| *v += 1).or_insert(1);
        } else {
            isolated2.entry(s).and_modify(|v| *v += 1).or_insert(1);
        }
    });
    let common_num = common.values().sum::<u64>() as f64;
    let isolated1_num = isolated1.values().sum::<u64>() as f64;
    let isolated2_num = isolated2.values().sum::<u64>() as f64;
    common_num / (isolated1_num + isolated2_num + common_num)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use petgraph::graph::NodeIndex;
    use petgraph::visit::EdgeRef;

    use crate::{parsing::PropertyGraphParser, property_graph::{Properties, PropertyGraph}};

    use super::{jaccard_index, weighted_jaccard_graphs};

    fn add_node(g: &mut PropertyGraph, name: &str) -> NodeIndex {
        g.graph.add_node(Properties { name: name.to_string(), map: HashMap::new() })
    }

    fn add_edge(g: &mut PropertyGraph, from: NodeIndex, to: NodeIndex, name: &str) {
        g.graph.add_edge(from, to, Properties { name: name.to_string(), map: HashMap::new() });
    }

    /// Documents a failure mode that only manifests if the unique-edge-name invariant
    /// (held by the .pgschema parser and by the transformation engine) is ever broken.
    /// Both graphs have 4 distinct nodes (A, B, C, D) and 4 edges, two named "e" and two
    /// named "f". The connectivity is permuted between the two edge classes so that:
    ///   G1: e: A->B, e: C->D, f: A->D, f: C->B   (A's out-neighbours: B via e, D via f)
    ///   G2: e: A->D, e: C->B, f: A->B, f: C->D   (A's out-neighbours: D via e, B via f)
    /// These schemas are structurally distinct, yet every local feature is shared with
    /// identical multiplicity because duplicated edge names erase the per-edge identity
    /// that `node:name:X;edge:name:Y` / `edge:name:Y;node:name:Z` would otherwise pin down.
    /// With unique edge names (as in real schemas) this collapse cannot happen.
    #[test]
    fn jaccard_one_on_duplicate_edge_names() {
        let mut g1 = PropertyGraph::default();
        let a1 = add_node(&mut g1, "A");
        let b1 = add_node(&mut g1, "B");
        let c1 = add_node(&mut g1, "C");
        let d1 = add_node(&mut g1, "D");
        add_edge(&mut g1, a1, b1, "e");
        add_edge(&mut g1, c1, d1, "e");
        add_edge(&mut g1, a1, d1, "f");
        add_edge(&mut g1, c1, b1, "f");

        let mut g2 = PropertyGraph::default();
        let a2 = add_node(&mut g2, "A");
        let b2 = add_node(&mut g2, "B");
        let c2 = add_node(&mut g2, "C");
        let d2 = add_node(&mut g2, "D");
        add_edge(&mut g2, a2, d2, "e");
        add_edge(&mut g2, c2, b2, "e");
        add_edge(&mut g2, a2, b2, "f");
        add_edge(&mut g2, c2, d2, "f");

        // Sanity check that the schemas really do differ: in G1, the edge of name "e"
        // out of A targets B; in G2 it targets D. So they cannot be name-preservingly
        // isomorphic. We verify this by looking at A's outgoing edges directly.
        let g1_a_out_e: Vec<&str> = g1.graph
            .edges_directed(a1, petgraph::EdgeDirection::Outgoing)
            .filter(|er| er.weight().name == "e")
            .map(|er| g1.graph.node_weight(er.target()).unwrap().name.as_str())
            .collect();
        let g2_a_out_e: Vec<&str> = g2.graph
            .edges_directed(a2, petgraph::EdgeDirection::Outgoing)
            .filter(|er| er.weight().name == "e")
            .map(|er| g2.graph.node_weight(er.target()).unwrap().name.as_str())
            .collect();
        assert_ne!(g1_a_out_e, g2_a_out_e, "graphs should differ structurally");

        // ...yet the similarity claims they are identical.
        let j = jaccard_index(&g1, &g2);
        let wj = weighted_jaccard_graphs(&g1, &g2, None, false);
        assert_eq!(j, 1.0, "jaccard_index should expose the collision");
        assert_eq!(wj, 1.0, "weighted_jaccard should expose the collision");
    }

    /// Same failure mode triggered by duplicated *node* names (the .pgschema parser
    /// silently collapses duplicates via `names.insert`, so this state isn't reachable
    /// from a parsed schema, but neither the type system nor the transformation engine
    /// rules out a hand-built `PropertyGraph` with repeated names).
    ///   G1: A1 -> A2 -> A3        (chain, degree sequence (out,in) = (1,0),(1,1),(0,1))
    ///   G2: A1 -> A2 <- A3        (collider, degree sequence (1,0),(0,2),(1,0))
    /// Each edge generates the same triple (A, e, A), so the multisets coincide.
    #[test]
    fn jaccard_one_on_duplicate_node_names() {
        let mut g1 = PropertyGraph::default();
        let n1 = add_node(&mut g1, "A");
        let n2 = add_node(&mut g1, "A");
        let n3 = add_node(&mut g1, "A");
        add_edge(&mut g1, n1, n2, "e");
        add_edge(&mut g1, n2, n3, "e");

        let mut g2 = PropertyGraph::default();
        let m1 = add_node(&mut g2, "A");
        let m2 = add_node(&mut g2, "A");
        let m3 = add_node(&mut g2, "A");
        add_edge(&mut g2, m1, m2, "e");
        add_edge(&mut g2, m3, m2, "e");

        let j = jaccard_index(&g1, &g2);
        let wj = weighted_jaccard_graphs(&g1, &g2, None, false);
        assert_eq!(j, 1.0);
        assert_eq!(wj, 1.0);
    }

    #[test]
    fn ibench_large_test() {
        let target_str = include_str!("../test_inputs/ibench_large_target.pgschema");
        let best_str = include_str!("../test_inputs/ibench_large_best.pgschema");
        let best = PropertyGraphParser.convert_text(best_str);
        let target = PropertyGraphParser.convert_text(target_str);
        let j = jaccard_index(&best[0], &target[0]);
        assert_eq!(j, 1.0);
    }
}
