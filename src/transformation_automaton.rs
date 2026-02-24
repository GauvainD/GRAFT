use std::collections::{HashMap, HashSet, VecDeque};

use petgraph::{
    graph::NodeIndex,
    prelude::StableGraph,
    visit::{EdgeRef, IntoEdgeReferences},
    Directed,
    Direction::{Incoming, Outgoing},
    Undirected,
};

use crate::{
    graph_transformation::GraphTransformation,
    property_graph::PropertyGraph,
    transformation::{Operation, OperationName},
};

#[derive(Clone, Debug)]
pub struct AutomatonNode {
    pub root: Operation,
    pub t_id: Option<usize>,
    pub op: Operation,
    pub group: Option<Vec<Operation>>,
}

pub struct TransformationAutomaton {
    pub start: Vec<NodeIndex>,
    pub node_set: HashMap<(Operation, Option<usize>), HashMap<Operation, NodeIndex>>,
    pub transfo_ids: HashMap<Option<usize>, String>,
    pub graph: StableGraph<AutomatonNode, Option<Operation>, Directed>,
}

impl TransformationAutomaton {
    pub fn new() -> Self {
        TransformationAutomaton {
            start: Vec::new(),
            node_set: HashMap::new(),
            transfo_ids: HashMap::new(),
            graph: StableGraph::new(),
        }
    }

    pub fn add_operation(
        &mut self,
        operation: &Operation,
        root: &Operation,
        t_id: Option<usize>,
        is_root: bool,
    ) -> NodeIndex {
        let mut added = false;
        let node_subset = if is_root {
            self.node_set
                .entry((root.clone(), t_id))
                .or_insert_with(|| {
                    added = true;
                    HashMap::new()
                })
        } else {
            self.node_set.get_mut(&(root.clone(), t_id)).unwrap()
        };
        let index = *node_subset.entry(operation.clone()).or_insert_with(|| {
            // println!("insert {:?}", operation);
            self.graph.add_node(AutomatonNode {
                root: root.clone(),
                t_id,
                op: operation.clone(),
                group: None,
            })
        });
        if is_root && added {
            self.start.push(index);
        }
        index
    }
}

impl Default for TransformationAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

fn to_undirected(
    g: &TransformationAutomaton,
) -> (
    StableGraph<(Operation, Option<usize>, Operation), (), Undirected>,
    HashMap<NodeIndex, NodeIndex>,
) {
    let mut new_graph: StableGraph<(Operation, Option<usize>, Operation), (), Undirected> =
        StableGraph::default();
    let mut node_map: HashMap<NodeIndex, NodeIndex> = HashMap::new();
    for node in g.graph.node_indices() {
        node_map.insert(
            node,
            new_graph.add_node((
                g.graph[node].root.clone(),
                g.graph[node].t_id,
                g.graph[node].op.clone(),
            )),
        );
    }
    for e in g.graph.edge_references() {
        let v1 = e.source();
        let v2 = e.target();
        let t1: OperationName = g.graph[v1].op.clone().into();
        let t2: OperationName = g.graph[v2].op.clone().into();
        let nv1 = node_map.get(&v1).unwrap();
        let nv2 = node_map.get(&v2).unwrap();
        if !new_graph.contains_edge(*nv1, *nv2) && t1 == t2 && g.graph.contains_edge(v2, v1) {
            new_graph.add_edge(*nv1, *nv2, ());
        }
    }
    (new_graph, node_map)
}

pub fn contract_graph(g: &mut TransformationAutomaton) {
    let (undirected, node_map) = to_undirected(g);
    let cliques = petgraph::algo::maximal_cliques(&undirected);
    let mut handled = HashSet::new();
    for set in cliques.into_iter().filter(|s| s.len() > 1) {
        let node1 = set.iter().next().unwrap();
        let (root, t_id, op) = &undirected[*node1];
        let mut new_node = AutomatonNode {
            root: root.clone(),
            t_id: *t_id,
            op: op.clone(),
            group: None,
        };
        let new_node_ref = g.graph.add_node(new_node);
        let mut group = vec![];
        for und_v in set.iter() {
            if !handled.contains(und_v) {
                let (root, t_id, op) = &undirected[*und_v];
                group.push(op.clone());
                let v = g
                    .node_set
                    .get(&(root.clone(), *t_id))
                    .unwrap()
                    .get(&op)
                    .unwrap();
                let neighbors_incoming = g.graph.neighbors_directed(*v, Incoming).detach();
                let neighbors_outgoing = g.graph.neighbors_directed(*v, Outgoing).detach();
                for (mut neighbor, incoming) in
                    [(neighbors_incoming, true), (neighbors_outgoing, false)]
                {
                    while let Some(u) = neighbor.next_node(&g.graph) {
                        if !set.contains(node_map.get(&u).unwrap()) {
                            if incoming && !g.graph.contains_edge(u, new_node_ref) {
                                g.graph.add_edge(u, new_node_ref, Some(op.clone()));
                            } else if !incoming && !g.graph.contains_edge(new_node_ref, u) {
                                g.graph.add_edge(new_node_ref, u, Some(op.clone()));
                            }
                        }
                    }
                }
                g.graph.remove_node(*v);
                g.node_set
                    .get_mut(&(root.clone(), *t_id))
                    .unwrap()
                    .insert(op.clone(), new_node_ref);
                handled.insert(*und_v);
            }
        }
        g.graph[new_node_ref].group = Some(group);
    }
}

pub struct SubsetGenerator {
    list: Vec<Operation>,
    base: GraphTransformation,
    current: Vec<GraphTransformation>,
    indices: Vec<usize>,
    index: usize,
}

impl SubsetGenerator {
    pub fn new(list: Vec<Operation>, base: GraphTransformation) -> Self {
        let mut indices = vec![0; list.len()];
        SubsetGenerator {
            list,
            base,
            current: Vec::new(),
            indices,
            index: 0,
        }
    }
}

impl Iterator for SubsetGenerator {
    type Item = GraphTransformation;

    fn next(&mut self) -> Option<Self::Item> {
        if self.list.is_empty() {
            None
        } else if self.current.is_empty() {
            let mut graph = self.base.clone();
            if graph.apply(&self.list[0]).is_some() {
                self.current.push(graph.clone());
                Some(graph)
            } else {
                None
            }
        } else if self.indices[0] == self.list.len() - 1 {
            None
        } else if self.indices[self.index] == self.list.len() - 1 {
            self.current.pop();
            self.index -= 1;
            self.indices[self.index] += 1;
            let mut graph = if self.index == 0 {
                self.base.clone()
            } else {
                self.current[self.index - 1].clone()
            };
            if graph.apply(&self.list[self.indices[self.index]]).is_some() {
                self.current[self.index] = graph.clone();
                Some(graph)
            } else {
                None
            }
        } else {
            self.indices[self.index + 1] = self.indices[self.index] + 1;
            self.index += 1;
            let mut graph = self.current[self.index - 1].clone();
            if graph.apply(&self.list[self.indices[self.index]]).is_some() {
                self.current.push(graph.clone());
                Some(graph)
            } else {
                None
            }
        }
    }
}

pub struct TransformGeneratorGraph {
    automaton: TransformationAutomaton,
    starts: VecDeque<NodeIndex>,
    list: VecDeque<(NodeIndex, GraphTransformation, usize)>,
    g: GraphTransformation,
    seen: HashSet<NodeIndex>,
    current_path: Vec<NodeIndex>,
    generator: Option<(NodeIndex, usize, SubsetGenerator)>,
}

impl TransformGeneratorGraph {
    pub fn new(automaton: TransformationAutomaton, g: &PropertyGraph) -> Self {
        for edge in automaton.graph.edge_references() {
            let src = automaton.graph[edge.source()].clone();
            let dst = automaton.graph[edge.target()].clone();
        }
        let starts = automaton.start.clone().into();
        TransformGeneratorGraph {
            automaton,
            starts,
            list: VecDeque::new(),
            g: g.into(),
            seen: HashSet::new(),
            current_path: Vec::new(),
            generator: None,
        }
    }

    fn start_list(&mut self) -> bool {
        if self.list.is_empty() {
            if self.starts.is_empty() {
                false
            } else {
                let start = self.starts.pop_front().unwrap();
                let mut g_clone = self.g.clone();
                let node = &self.automaton.graph[start];
                g_clone.transfo_id = self.automaton.transfo_ids.get(&node.t_id).cloned();
                g_clone.root = Some(node.root.clone());
                self.list.push_back((start, g_clone, 0));
                true
            }
        } else {
            true
        }
    }
}

impl Iterator for TransformGeneratorGraph {
    type Item = GraphTransformation;

    fn next(&mut self) -> Option<Self::Item> {
        while self.generator.is_some() || self.start_list() {
            if let Some((node, depth, generator)) = self.generator.as_mut() {
                if let Some(g) = generator.next() {
                    let mut neighbors = self.automaton.graph.neighbors(*node).detach();
                    while let Some(neighbor) = neighbors.next_node(&self.automaton.graph) {
                        let ng = g.clone();
                        self.list.push_back((neighbor, ng, *depth + 1));
                    }
                    return Some(g);
                } else {
                    self.generator = None;
                }
            } else {
                let (current, mut g, depth) = self.list.pop_back().unwrap();
                for node in self.current_path.drain(depth..) {
                    self.seen.remove(&node);
                }
                if self.seen.contains(&current) {
                    return Some(g);
                }
                if let Some(group) = self.automaton.graph[current].group.clone() {
                    self.seen.insert(current.clone());
                    self.current_path.push(current.clone());
                    let generator = SubsetGenerator::new(group, g);
                    self.generator = Some((current, depth, generator));
                } else if g.apply(&self.automaton.graph[current].op).is_some() {
                    self.seen.insert(current.clone());
                    self.current_path.push(current.clone());
                    let mut neighbor_count = 0;
                    let mut neighbors = self.automaton.graph.neighbors(current).detach();
                    while let Some(neighbor) = neighbors.next_node(&self.automaton.graph) {
                        neighbor_count += 1;
                        let ng = g.clone();
                        self.list.push_back((neighbor, ng, depth + 1));
                    }
                    if neighbor_count == 0 {
                        return Some(g);
                    }
                }
            }
        }
        None
    }
}
