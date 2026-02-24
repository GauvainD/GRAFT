use std::{collections::HashMap, fmt::Display};

use log::error;
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    visit::{EdgeRef, IntoEdgeReferences, IntoNodeReferences},
};

use crate::{
    constants::IDEMPOTENCE, property_graph::{Properties, PropertyGraph}, transformation::Operation
};

#[derive(Debug)]
pub struct GraphTransformation {
    pub init: PropertyGraph,
    pub result: PropertyGraph,
    pub operations: Vec<String>,
    pub transfo_id: Option<String>,
    pub root: Option<Operation>,
    node_map: HashMap<u32, NodeIndex<u32>>,
    edge_map: HashMap<u32, EdgeIndex<u32>>,
    node_label_map: HashMap<u32, u32>,
    edge_label_map: HashMap<u32, u32>,
    node_ids: HashMap<String, NodeIndex<u32>>,
    edge_ids: HashMap<String, EdgeIndex<u32>>,
    label_node_ids: HashMap<String, u32>,
    label_edge_ids: HashMap<String, u32>,
}

impl From<&PropertyGraph> for GraphTransformation {
    fn from(g: &PropertyGraph) -> Self {
        GraphTransformation {
            init: g.clone(),
            result: g.clone(),
            operations: Vec::new(),
            transfo_id: None,
            root: None,
            node_map: HashMap::new(),
            edge_map: HashMap::new(),
            node_label_map: HashMap::new(),
            edge_label_map: HashMap::new(),
            node_ids: g
                .graph
                .node_references()
                .map(|(index, props)| (props.name.clone(), index))
                .collect(),
            edge_ids: g
                .graph
                .edge_references()
                .map(|e| (e.weight().name.clone(), e.id()))
                .collect(),
            label_node_ids: g
                .vertex_label
                .labels()
                .map(|&id| (g.vertex_label.get_label(id).unwrap().clone(), id))
                .collect(),
            label_edge_ids: g
                .edge_label
                .labels()
                .map(|&id| (g.edge_label.get_label(id).unwrap().clone(), id))
                .collect(),
        }
    }
}

impl Display for GraphTransformation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "===")?;
        write!(f, "{}", self.init)?;
        writeln!(f, "---")?;
        write!(f, "{}", self.result)?;
        writeln!(f, "===")
    }
}

impl Clone for GraphTransformation {
    fn clone(&self) -> Self {
        Self {
            init: self.init.clone(),
            result: self.result.clone(),
            operations: self.operations.clone(),
            root: self.root.clone(),
            transfo_id: self.transfo_id.clone(),
            node_map: self.node_map.clone(),
            edge_map: self.edge_map.clone(),
            node_label_map: self.node_label_map.clone(),
            edge_label_map: self.edge_label_map.clone(),
            node_ids: self.node_ids.clone(),
            edge_ids: self.edge_ids.clone(),
            label_node_ids: self.label_node_ids.clone(),
            label_edge_ids: self.label_edge_ids.clone(),
        }
    }
}

impl GraphTransformation {
    fn get_node_index(&self, id: &u32) -> NodeIndex<u32> {
        *self.node_map.get(&id).unwrap_or(&(*id).into())
    }

    fn get_edge_index(&self, id: &u32) -> EdgeIndex<u32> {
        *self.edge_map.get(&id).unwrap_or(&(*id).into())
    }

    fn get_node_label_index(&self, id: &u32) -> u32 {
        *self.node_label_map.get(id).unwrap_or(id)
    }

    fn get_edge_label_index(&self, id: &u32) -> u32 {
        *self.edge_label_map.get(id).unwrap_or(id)
    }

    pub fn apply(&mut self, op: &Operation) -> Option<()> {
        let idempotent = IDEMPOTENCE.get().unwrap();
        match op {
            Operation::AddVertexLabel(v, l) => {
                let vertex = self.node_ids.get(v)?;
                let label = match self.label_node_ids.get(l) {
                    Some(label) => *label,
                    None => {
                        let label = self.result.vertex_label.add_label(l.to_string());
                        self.label_node_ids.insert(l.to_string(), label);
                        label
                    }
                };
                self.result
                    .vertex_label
                    .add_label_mapping(vertex, label)
                    .ok()?;
            }
            Operation::RemoveVertexLabel(v, l) => {
                let vertex = self.node_ids.get(v);
                let label = self.label_node_ids.get(l);
                if let (Some(vertex), Some(label)) = (vertex, label) {
                    let res = self.result
                        .vertex_label
                        .remove_label_mapping(vertex, *label)
                        .ok();
                    if !idempotent && res.is_none() {
                        return None;
                    }
                } else if !idempotent && (vertex.is_none() || label.is_none()) {
                    return None;
                }
            }
            Operation::AddEdgeLabel(e, l) => {
                let edge = self.edge_ids.get(e)?;
                let label = match self.label_edge_ids.get(l) {
                    Some(label) => *label,
                    None => {
                        let label = self.result.edge_label.add_label(l.to_string());
                        self.label_edge_ids.insert(l.to_string(), label);
                        label
                    }
                };
                self.result.edge_label.add_label_mapping(edge, label).ok()?;
            }
            Operation::RemoveEdgeLabel(e, l) => {
                let edge = self.edge_ids.get(e);
                let label = self.label_edge_ids.get(l);
                if let (Some(edge), Some(label)) = (edge, label) {
                    let res = self.result
                        .edge_label
                        .remove_label_mapping(edge, *label)
                        .ok();
                    if !idempotent && res.is_none() {
                        return None;
                    }
                } else if !idempotent && (edge.is_none() || label.is_none()) {
                    return None;
                }
            }
            Operation::AddVertex(v) => {
                if self.node_ids.contains_key(v) {
                    // error!("Node {v} already exists.");
                    if !idempotent {
                        return None;
                    }
                } else {
                    let real_index = self.result.graph.add_node(Properties {
                        name: v.clone(),
                        map: HashMap::new(),
                    });
                    self.node_ids.insert(v.clone(), real_index);
                }
            }
            Operation::RemoveVertex(v) => {
                if let Some(index) = self.node_ids.get(v) {
                    self.result.vertex_label.remove_element(index);
                    self.result.graph.remove_node(*index);
                    self.node_ids.remove(v);
                } else if !idempotent {
                    return None;
                }
            }
            Operation::AddEdge(e, start, end) => {
                if self.edge_ids.contains_key(e) {
                    // error!("Edge {e} already exists.");
                    if !idempotent {
                        return None;
                    }
                } else {
                    let n1 = self.node_ids.get(start)?;
                    let n2 = self.node_ids.get(end)?;
                    let real_index = self.result.graph.add_edge(
                        *n1,
                        *n2,
                        Properties {
                            name: e.clone(),
                            map: HashMap::new(),
                        },
                    );
                    self.edge_ids.insert(e.clone(), real_index);
                }
            }
            Operation::RemoveEdge(e) => {
                if let Some(index) =  self.edge_ids.get(e) {
                    self.result.edge_label.remove_element(index);
                    self.result.graph.remove_edge(*index);
                    self.edge_ids.remove(e);
                } else if !idempotent {
                    return None;
                }
            }
            Operation::AddVertexProperty(v, name, value) => {
                let prop = self.result.graph.node_weight_mut(*self.node_ids.get(v)?)?;
                prop.map.insert(name.to_string(), value.to_string());
            }
            Operation::RemoveVertexProperty(v, name) => {
                let prop = self.result.graph.node_weight_mut(*self.node_ids.get(v)?)?;
                prop.map.remove(name);
            }
            Operation::AddEdgeProperty(e, name, value) => {
                let prop = self.result.graph.edge_weight_mut(*self.edge_ids.get(e)?)?;
                prop.map.insert(name.to_string(), value.to_string());
            }
            Operation::RemoveEdgeProperty(e, name) => {
                let prop = self.result.graph.edge_weight_mut(*self.edge_ids.get(e)?)?;
                prop.map.remove(name);
            }
            Operation::RenameVertex(v, name) => {
                if let Some(id) = self.node_ids.get(v) {
                    let prop = self.result.graph.node_weight_mut(*id)?;
                    prop.name.clone_from(name);
                } else if !idempotent {
                    return None;
                }
            }
            Operation::RenameEdge(e, name) => {
                if let Some(id) = self.edge_ids.get(e) {
                    let prop = self.result.graph.edge_weight_mut(*id)?;
                    prop.name.clone_from(name);
                } else if !idempotent {
                    return None;
                }
            }
            Operation::MoveEdgeTarget(e, t) => {
                let edgeindex = self.edge_ids.get(e)?;
                let src = self.result.graph.edge_endpoints(*edgeindex)?.0;
                let target = self.node_ids.get(t)?;
                let w = self.result.graph.remove_edge(*edgeindex)?;
                let new_index = self.result.graph.add_edge(src, *target, w);
                let labels: Vec<u32> = self
                    .result
                    .edge_label
                    .element_labels(edgeindex)
                    .copied()
                    .collect();
                labels.into_iter().try_for_each(|l| {
                    self.result.edge_label.add_label_mapping(&new_index, l).ok()
                })?;
                self.result.edge_label.remove_element(edgeindex);
                self.edge_ids.insert(e.clone(), new_index);
            }
            Operation::MoveEdgeSource(e, s) => {
                let edgeindex = self.edge_ids.get(e)?;
                let target = self.result.graph.edge_endpoints(*edgeindex)?.1;
                let src = self.node_ids.get(s)?;
                let w = self.result.graph.remove_edge(*edgeindex)?;
                let new_index = self.result.graph.add_edge(*src, target, w);
                let labels: Vec<u32> = self
                    .result
                    .edge_label
                    .element_labels(edgeindex)
                    .copied()
                    .collect();
                labels.into_iter().try_for_each(|l| {
                    self.result.edge_label.add_label_mapping(&new_index, l).ok()
                })?;
                self.result.edge_label.remove_element(edgeindex);
                self.edge_ids.insert(e.clone(), new_index);
            }
        }
        self.operations.push(format!("{:?}", op));
        Some(())
    }

}
