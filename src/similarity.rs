//! Module dedicated to handling similarity between property graph schemas
use std::{collections::HashMap, fmt::format};

use petgraph::graph::{EdgeIndex, NodeIndex};
use probminhash::probminhasher::ProbMinHash3aSha;

use crate::property_graph::PropertyGraph;

/// Extracts features from a node. One feature per node type/name, property and label
pub fn node_base_features(g: &PropertyGraph, n: &NodeIndex) -> Vec<String> {
    let mut features = Vec::new();
    let weight = g.graph.node_weight(*n).unwrap();
    features.push(format!("node:name:{}",weight.name));
    for prop in weight.map.iter() {
        features.push(format!("node:prop:{}:{}",prop.0,prop.1));
    }
    for label in g.vertex_label.element_labels(n).map(|id| g.vertex_label.get_label(*id).unwrap()) {
        features.push(format!("node:label:{}",label));
    }
    features
}

/// Extracts features from the data of an edge. One feature per node type/name, property and label
pub fn edge_base_features(g: &PropertyGraph, e: &EdgeIndex) -> Vec<String> {
    let mut features = Vec::new();
    let weight = g.graph.edge_weight(*e).unwrap();
    features.push(format!("edge:name:{}",weight.name));
    for prop in weight.map.iter() {
        features.push(format!("edge:prop:{}:{}",prop.0,prop.1));
    }
    for label in g.edge_label.element_labels(e).map(|id| g.edge_label.get_label(*id).unwrap()) {
        features.push(format!("edge:label:{}",label));
    }
    features
}

/// Generate features from the features of a node or edge with no regard to connectivity (only data
/// of the element). See pair_features and adj_features for other features.
pub fn inner_features(base_features: &[String]) -> Vec<String> {
    let mut res: Vec<String> = base_features.iter().cloned().collect();
    for i in 0..base_features.len() {
        for j in 0..base_features.len() {
            if i != j {
                res.push(format!("inner:{};{}", base_features[i], base_features[j]));
            }
        }
    }
    res
}

/// Generate features from two sets of features. For example, a node and an incident edge.
pub fn pair_features(first_features: &[String], second_features: &[String], prefix: &str) -> Vec<String> {
    let mut res = Vec::new();
    for f1 in first_features.iter() {
        for f2 in second_features.iter() {
            res.push(format!("{}{};{}",prefix,f1,f2));
        }
    }
    res
}

/// Generates all features from an edge and its incident nodes. Pairs are created between features
/// of the two nodes and features of the edge with each node.
pub fn adj_features(from_features: &[String], to_features: &[String], edge_features: &[String]) -> Vec<String> {
    pair_features(from_features, to_features, "adj:").into_iter()
        .chain(pair_features(from_features, edge_features, "").into_iter())
        .chain(pair_features(edge_features, to_features, "").into_iter())
        .collect()
}

/// Extracts features from a property graph that include information about adjacency as well as
/// data.
pub fn property_graph_features(g: &PropertyGraph) -> Vec<String> {
    let node_features: HashMap<NodeIndex, Vec<String>> = g.graph.node_indices().map(|id| (id,node_base_features(g, &id))).collect();
    g.graph.node_indices().flat_map(|id| inner_features(node_features.get(&id).unwrap()).into_iter())
        .chain(g.graph.edge_indices().flat_map(|id| {
            let ef = edge_base_features(g, &id);
            let (from,to) = g.graph.edge_endpoints(id).unwrap();
            let ff = node_features.get(&from).unwrap();
            let tf = node_features.get(&to).unwrap();
            inner_features(&ef).into_iter().chain(adj_features(&ff, &tf, &ef).into_iter())
        }))
        .collect()
}

/// Computes the minhash signature of a property graph schema with a sample of size `sample`.
/// Features are weighted with their frequency of occurrence.
pub fn property_graph_minhash(g: &PropertyGraph, sample: usize) -> Vec<String> {
    let features = property_graph_features(g).into_iter().fold(HashMap::new(), |mut map, feature| {
        *map.entry(feature).or_insert(0) += 1;
        map
    });
    let mut minhash = ProbMinHash3aSha::new(sample, "".to_string());
    minhash.hash_weigthed_hashmap(&features);
    minhash.get_signature().to_vec()
}

/// Computes the Jaccard index between two property graph schemas from their respective sets of
/// features. Features are weighted with their frequency of occurrence.
pub fn jaccard_index(g1: &PropertyGraph, g2: &PropertyGraph) -> f64 {
    let mut isolated1 = property_graph_features(g1)
        .into_iter()
        .fold(HashMap::new(), |mut h, s| {
            h.entry(s).and_modify(|v| *v+=1).or_insert(1);
            h
    });
    let mut common = HashMap::new();
    let mut isolated2 = HashMap::new();
    property_graph_features(g2).into_iter().for_each(|s| {
        let num1 = isolated1.get_mut(&s).map(|v| {*v-=1; v});
        if let Some(num1) = num1 {
            if *num1 == 0u64 {
                isolated1.remove(&s);
            }
            common.entry(s).and_modify(|v| *v+=1).or_insert(1);
        } else {
            isolated2.entry(s).and_modify(|v| *v+=1).or_insert(1);
        }
    });
    let common_num = common.values().sum::<u64>() as f64;
    let isolated1_num = isolated1.values().sum::<u64>() as f64;
    let isolated2_num = isolated2.values().sum::<u64>() as f64;
    common_num / (isolated1_num + isolated2_num + common_num)
}
