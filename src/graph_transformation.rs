//! Module dedicated to GraphTransformation. These represent a schema being transformed and the
//! result of a transformation.
use std::{collections::HashMap, fmt::Display, sync::Arc};

use log::error;
use petgraph::{
    graph::{EdgeIndex, NodeIndex},
    visit::{EdgeRef, IntoEdgeReferences, IntoNodeReferences},
};

use crate::{
    constants::IDEMPOTENCE,
    constraints::{Constraint, PoolEntry},
    property_graph::{Properties, PropertyGraph},
    transformation::Operation,
};

/// All data for a single transformation.
#[derive(Debug)]
pub struct GraphTransformation {
    /// The initial schema being transformed
    pub init: PropertyGraph,
    /// The resulting schema
    pub result: PropertyGraph,
    /// The list of operations applied in order to the initial schema
    pub operations: Vec<String>,
    /// If provided, the id of the meta-transformation
    pub transfo_id: Option<String>,
    /// If provided, the first edit operation (obtained from the Start relation)
    pub root: Option<Operation>,
    /// Maps each node name to the corresponding index in the graph
    node_ids: HashMap<String, NodeIndex<u32>>,
    /// Maps each edge name to the corresponding index in the graph
    edge_ids: HashMap<String, EdgeIndex<u32>>,
    /// Maps each node label to its index
    label_node_ids: HashMap<String, u32>,
    /// Maps each edge label to its index
    label_edge_ids: HashMap<String, u32>,
    /// Constraints that can be added or removed, by identifier (their canonical text when the
    /// transformation started). Shared between all the transformations of a same schema.
    pub constraints: Arc<HashMap<String, PoolEntry>>,
}

impl From<&PropertyGraph> for GraphTransformation {
    /// Produces a GraphTransformation from a PropertyGraph
    fn from(g: &PropertyGraph) -> Self {
        GraphTransformation {
            init: g.clone(),
            result: g.clone(),
            operations: Vec::new(),
            transfo_id: None,
            root: None,
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
            constraints: Arc::new(
                g.constraints
                    .iter()
                    .map(|c| (c.text(g, true), c.to_pool_entry(g)))
                    .collect(),
            ),
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
            node_ids: self.node_ids.clone(),
            edge_ids: self.edge_ids.clone(),
            label_node_ids: self.label_node_ids.clone(),
            label_edge_ids: self.label_edge_ids.clone(),
            constraints: self.constraints.clone(),
        }
    }
}

impl GraphTransformation {
    /// Makes the constraints of the target schema available to `AddConstraint`.
    pub fn add_target_constraints(&mut self, target: &PropertyGraph) {
        if !target.constraints.is_empty() {
            let pool = Arc::make_mut(&mut self.constraints);
            for c in target.constraints.iter() {
                pool.entry(c.text(target, true))
                    .or_insert_with(|| c.to_pool_entry(target));
            }
        }
    }

    /// Binds a constraint of the pool to the resulting schema. Types are first looked up by the
    /// name they had when the transformation started, then by their current name.
    fn bind_constraint(&self, ident: &str) -> Option<Constraint> {
        let g = &self.result.graph;
        self.constraints
            .get(ident)?
            .bind(
                |name| {
                    self.node_ids
                        .get(name)
                        .copied()
                        .or_else(|| g.node_indices().find(|n| g[*n].name == name))
                },
                |name| {
                    self.edge_ids
                        .get(name)
                        .copied()
                        .or_else(|| g.edge_indices().find(|e| g[*e].name == name))
                },
            )
            .ok()
    }

    /// Changes the endpoints of an edge. The edge gets a new index. Fails if a constraint does
    /// not hold on the moved edge.
    fn move_edge(&mut self, e: &str, source: Option<&str>, target: Option<&str>) -> Option<()> {
        let old = *self.edge_ids.get(e)?;
        let (mut src, mut dst) = self.result.graph.edge_endpoints(old)?;
        if let Some(source) = source {
            src = *self.node_ids.get(source)?;
        }
        if let Some(target) = target {
            dst = *self.node_ids.get(target)?;
        }
        let w = self.result.graph.remove_edge(old)?;
        let new = self.result.graph.add_edge(src, dst, w);
        let labels: Vec<u32> = self.result.edge_label.element_labels(&old).copied().collect();
        labels
            .into_iter()
            .try_for_each(|l| self.result.edge_label.add_label_mapping(&new, l).ok())?;
        self.result.edge_label.remove_element(&old);
        self.edge_ids.insert(e.to_string(), new);
        let mut constraints = std::mem::take(&mut self.result.constraints);
        let mut valid = true;
        for c in constraints.iter_mut().filter(|c| c.uses_edge(old)) {
            c.replace_edge(old, new);
            valid = valid && c.validate(&self.result);
        }
        self.result.constraints = constraints;
        valid.then_some(())
    }

    /// Transforms the resulting schema by applying the given operation. If IDEMPOTENCE is set to
    /// true, operations that do not change the schema will be ignored. Otherwise, None will be
    /// returned.
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
                    if self.result.constraints.iter().any(|c| c.uses_node_label(*vertex, l)) {
                        return None;
                    }
                    let res = self
                        .result
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
                    if self.result.constraints.iter().any(|c| c.uses_edge_label(*edge, l)) {
                        return None;
                    }
                    let res = self
                        .result
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
                    if self.result.constraints.iter().any(|c| c.uses_node(*index)) {
                        return None;
                    }
                    self.result.vertex_label.remove_element(index);
                    self.result.graph.remove_node(*index);
                    self.node_ids.remove(v);
                } else if !idempotent {
                    return None;
                }
            }
            Operation::AddEdge(e, start, end) => {
                if self.edge_ids.contains_key(e) {
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
                if let Some(index) = self.edge_ids.get(e) {
                    if self.result.constraints.iter().any(|c| c.uses_edge(*index)) {
                        return None;
                    }
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
                let id = *self.node_ids.get(v)?;
                if self.result.constraints.iter().any(|c| c.uses_node_prop(id, name)) {
                    return None;
                }
                let prop = self.result.graph.node_weight_mut(id)?;
                prop.map.remove(name);
            }
            Operation::AddEdgeProperty(e, name, value) => {
                let prop = self.result.graph.edge_weight_mut(*self.edge_ids.get(e)?)?;
                prop.map.insert(name.to_string(), value.to_string());
            }
            Operation::RemoveEdgeProperty(e, name) => {
                let id = *self.edge_ids.get(e)?;
                if self.result.constraints.iter().any(|c| c.uses_edge_prop(id, name)) {
                    return None;
                }
                let prop = self.result.graph.edge_weight_mut(id)?;
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
            Operation::MoveEdgeTarget(e, t) => self.move_edge(e, None, Some(t))?,
            Operation::MoveEdgeSource(e, s) => self.move_edge(e, Some(s), None)?,
            Operation::AddConstraint(ident) => {
                let constraint = self.bind_constraint(ident)?;
                if !constraint.validate(&self.result) {
                    return None;
                }
                let result = &self.result;
                if result.constraints.iter().any(|c| c.same_as(&constraint, result)) {
                    if !idempotent {
                        return None;
                    }
                } else {
                    self.result.constraints.push(constraint);
                }
            }
            Operation::RemoveConstraint(ident) => {
                let result = &self.result;
                let position = self.bind_constraint(ident).and_then(|constraint| {
                    result
                        .constraints
                        .iter()
                        .position(|c| c.same_as(&constraint, result))
                });
                match position {
                    Some(position) => {
                        self.result.constraints.swap_remove(position);
                    }
                    None if !idempotent => return None,
                    None => (),
                }
            }
        }
        self.operations.push(format!("{:?}", op));
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::PropertyGraphParser;

    const SOURCE: &str = "CREATE GRAPH TYPE {
        (person: Person { id INT, name STRING }),
        (city: City { name STRING }),
        (:person)-[lives: LivesIn { since INT }]->(:city),
        FOR (x: person) EXCLUSIVE MANDATORY SINGLETON x.id
    }";

    const TARGET: &str = "CREATE GRAPH TYPE {
        (person: Person { id INT, name STRING }),
        (city: City { name STRING }),
        (:person)-[lives: LivesIn { since INT }]->(:city),
        FOR (p: person) MANDATORY l.since WITHIN (p)-[l: lives]->(: city)
    }";

    fn parse(text: &str) -> PropertyGraph {
        PropertyGraphParser.convert_text(text).remove(0)
    }

    fn transformation() -> (GraphTransformation, String, String) {
        let _ = IDEMPOTENCE.set(false);
        let source = parse(SOURCE);
        let target = parse(TARGET);
        let mut gt: GraphTransformation = (&source).into();
        gt.add_target_constraints(&target);
        let key = source.constraints[0].text(&source, true);
        let ggd = target.constraints[0].text(&target, true);
        (gt, key, ggd)
    }

    fn op1(f: fn(String) -> Operation, a: &str) -> Operation {
        f(a.to_string())
    }

    fn op2(f: fn(String, String) -> Operation, a: &str, b: &str) -> Operation {
        f(a.to_string(), b.to_string())
    }

    #[test]
    fn add_and_remove_constraints() {
        let (mut gt, key, ggd) = transformation();
        assert_eq!(gt.constraints.len(), 2);
        assert!(gt.apply(&Operation::AddConstraint(ggd.clone())).is_some());
        assert_eq!(gt.result.constraints.len(), 2);
        // Already there
        assert!(gt.apply(&Operation::AddConstraint(ggd.clone())).is_none());
        assert!(gt.apply(&Operation::AddConstraint(key.clone())).is_none());
        assert!(gt.apply(&Operation::RemoveConstraint(key.clone())).is_some());
        assert!(gt.apply(&Operation::RemoveConstraint(key.clone())).is_none());
        assert_eq!(gt.result.constraints.len(), 1);
        assert_eq!(gt.result.constraints[0].text(&gt.result, true), ggd);
        assert!(gt.apply(&Operation::AddConstraint("unknown".to_string())).is_none());
    }

    #[test]
    fn rename_keeps_constraints() {
        let (mut gt, key, ggd) = transformation();
        assert!(gt.apply(&op2(Operation::RenameVertex, "person", "human")).is_some());
        assert_eq!(
            gt.result.constraints[0].text(&gt.result, false),
            "FOR (x:human) EXCLUSIVE MANDATORY SINGLETON x.id"
        );
        assert!(gt.result.constraints[0].validate(&gt.result));
        // Identifiers are those of the schema before the transformation
        assert!(gt.apply(&Operation::AddConstraint(ggd)).is_some());
        assert!(gt.apply(&Operation::RemoveConstraint(key)).is_some());
        assert_eq!(
            gt.result.constraints[0].text(&gt.result, false),
            "FOR (p:human) MANDATORY l.since WITHIN (p)-[l:lives]->(:city)"
        );
    }

    #[test]
    fn used_elements_cannot_be_removed() {
        let (mut gt, key, ggd) = transformation();
        assert!(gt.apply(&Operation::AddConstraint(ggd.clone())).is_some());
        assert!(gt.clone().apply(&op1(Operation::RemoveVertex, "person")).is_none());
        assert!(gt.clone().apply(&op1(Operation::RemoveEdge, "lives")).is_none());
        assert!(gt.clone().apply(&op2(Operation::RemoveVertexProperty, "person", "id")).is_none());
        assert!(gt.clone().apply(&op2(Operation::RemoveEdgeProperty, "lives", "since")).is_none());
        // Not used by any constraint
        assert!(gt.apply(&op2(Operation::RemoveVertexProperty, "person", "name")).is_some());
        // Once the constraints are removed, everything can go
        assert!(gt.apply(&Operation::RemoveConstraint(ggd)).is_some());
        assert!(gt.apply(&op2(Operation::RemoveEdgeProperty, "lives", "since")).is_some());
        assert!(gt.apply(&op1(Operation::RemoveEdge, "lives")).is_some());
        assert!(gt.apply(&Operation::RemoveConstraint(key)).is_some());
        assert!(gt.apply(&op1(Operation::RemoveVertex, "person")).is_some());
    }

    #[test]
    fn moving_an_edge_keeps_or_rejects_constraints() {
        let (mut gt, _, ggd) = transformation();
        assert!(gt.apply(&Operation::AddConstraint(ggd)).is_some());
        // The pattern needs the edge to start from person and to end in city
        assert!(gt.clone().apply(&op2(Operation::MoveEdgeSource, "lives", "city")).is_none());
        assert!(gt.clone().apply(&op2(Operation::MoveEdgeTarget, "lives", "person")).is_none());
        // Moving to the same endpoint changes the index of the edge only
        assert!(gt.apply(&op2(Operation::MoveEdgeTarget, "lives", "city")).is_some());
        assert!(gt.result.constraints.iter().all(|c| c.validate(&gt.result)));
    }

    #[test]
    fn constraint_needs_its_types() {
        let (mut gt, key, ggd) = transformation();
        assert!(gt.apply(&Operation::RemoveConstraint(key)).is_some());
        assert!(gt.apply(&op2(Operation::RemoveEdgeProperty, "lives", "since")).is_some());
        assert!(gt.clone().apply(&Operation::AddConstraint(ggd.clone())).is_none());
        assert!(gt.apply(&Operation::AddEdgeProperty("lives".into(), "since".into(), "INT".into())).is_some());
        assert!(gt.apply(&Operation::AddConstraint(ggd)).is_some());
    }

    #[test]
    fn constraints_change_hash_and_similarity() {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let hash = |g: &PropertyGraph| {
            let mut h = DefaultHasher::new();
            g.hash(&mut h);
            h.finish()
        };
        let (mut gt, _, ggd) = transformation();
        let target = parse(TARGET);
        let before = crate::similarity::jaccard_index(&gt.result, &target);
        let hash_before = hash(&gt.result);
        assert!(gt.apply(&Operation::AddConstraint(ggd)).is_some());
        assert_ne!(hash_before, hash(&gt.result));
        assert!(crate::similarity::jaccard_index(&gt.result, &target) > before);
    }
}
