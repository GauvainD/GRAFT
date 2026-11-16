//! Constraints over property graph schemas.
//!
//! A single notation covers pg-keys and graph generating dependencies (GGD):
//!
//! ```text
//! FOR <matches> [WHERE <conditions>] <qualifier>+ [<keys>] [WITHIN <matches>] [WHERE <conditions>]
//! ```
//!
//! where `<matches>` is a conjunction of node and edge matches sharing variables. The named
//! variables of the `FOR` side are universally quantified, the variables that only appear in the
//! `WITHIN` side are existential.
//!
//! A constraint is split in two parts so that transformations stay cheap:
//! - [`ConstraintDef`] is the parsed structure. It is immutable, shared through an [`Arc`] and
//!   contains no type name: types are referenced through numbered *slots*.
//! - [`Constraint`] binds each slot to a node or edge index of one schema. This is the only part
//!   that is cloned and checked while applying edit operations, and it contains no string, so
//!   renaming a type does not touch it.
//!
//! Text (for printing, Datalog and similarity) is only produced on demand from a constraint and
//! the schema it is bound to.
use std::{collections::HashMap, fmt::Write, sync::Arc};

use petgraph::graph::{EdgeIndex, NodeIndex};
use smallvec::SmallVec;
use thiserror::Error;

use crate::property_graph::PropertyGraph;

/// Index of a variable in [`ConstraintDef::vars`].
pub type VarId = u16;
/// Index of a type slot in [`ConstraintDef::node_slots`] or [`ConstraintDef::edge_slots`].
pub type SlotId = u16;

/// Name of the `FOR` side in the Datalog encoding.
pub const SIDE_FOR: &str = "FOR";
/// Name of the `WITHIN` side in the Datalog encoding.
pub const SIDE_WITHIN: &str = "WITHIN";

/// Errors raised while building a constraint.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConstraintError {
    #[error("Variable {0} is used both as a node and as an edge")]
    KindMismatch(String),
    #[error("Variable {0} is given two different types: {1} and {2}")]
    TypeMismatch(String, String, String),
    #[error("Variable {0} is not bound by any match")]
    UnknownVariable(String),
    #[error("Unknown node type {0}")]
    UnknownNodeType(String),
    #[error("Unknown edge type {0}")]
    UnknownEdgeType(String),
    #[error("Invalid constraint syntax: {0}")]
    Syntax(String),
}

/// A variable of a constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarInfo {
    /// Name given in the text (empty for anonymous variables).
    pub name: String,
    /// True if the variable was not named. Anonymous variables are existential.
    pub anonymous: bool,
    /// True if the variable matches an edge.
    pub edge: bool,
    /// Slot of the type of the variable, if it is typed.
    pub slot: Option<SlotId>,
    /// Labels required on the variable.
    pub labels: Vec<String>,
}

/// One match of a pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Atom {
    /// `(x)`
    Node(VarId),
    /// `(from)-[var]->(to)`
    Edge { var: VarId, from: VarId, to: VarId },
}

/// Operand of a condition or element of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operand {
    /// The element bound to a variable.
    Var(VarId),
    /// A property of the element bound to a variable.
    Prop(VarId, String),
    /// A constant, kept as written.
    Const(String),
}

/// One side of a comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    Simple(Operand),
    /// `DIST(a, b)`, the distance used by differential constraints.
    Dist(Operand, Operand),
}

/// A comparison between two terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    pub left: Term,
    /// One of `=`, `<>`, `<`, `>`, `<=`, `>=`.
    pub op: String,
    pub right: Term,
}

/// A pattern (conjunction of matches) and the conditions on its variables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Side {
    pub atoms: Vec<Atom>,
    pub conditions: Vec<Condition>,
}

/// What a constraint requires from the type bound to a slot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotReq {
    /// Properties that must exist on the type.
    pub props: Vec<String>,
    /// Labels that the type must have.
    pub labels: Vec<String>,
}

/// The structure of a constraint, independent of any schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConstraintDef {
    pub exclusive: bool,
    pub mandatory: bool,
    pub singleton: bool,
    /// The key tuple (variables and properties only).
    pub keys: Vec<Operand>,
    /// Variables, in order of first occurrence (`FOR` side first).
    pub vars: Vec<VarInfo>,
    /// The `FOR` side.
    pub lhs: Side,
    /// The `WITHIN` side.
    pub rhs: Side,
    /// Requirements for each node type referenced.
    pub node_slots: Vec<SlotReq>,
    /// Requirements for each edge type referenced.
    pub edge_slots: Vec<SlotReq>,
}

/// A constraint of a schema: a definition and the schema elements its slots refer to.
#[derive(Debug, Clone)]
pub struct Constraint {
    pub def: Arc<ConstraintDef>,
    /// Node of the schema for each node slot.
    pub nodes: SmallVec<[NodeIndex; 4]>,
    /// Edge of the schema for each edge slot.
    pub edges: SmallVec<[EdgeIndex; 4]>,
}

/// A constraint that is not bound to a schema yet: slots are given by type name. Used for the
/// constraints that edit operations can add.
#[derive(Debug, Clone)]
pub struct PoolEntry {
    pub def: Arc<ConstraintDef>,
    /// Name of the node type of each node slot.
    pub nodes: Vec<String>,
    /// Name of the edge type of each edge slot.
    pub edges: Vec<String>,
}

/// The Datalog facts describing a constraint. Variable names are the canonical ones.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConstraintFacts {
    /// Identifier of the constraint (its canonical text).
    pub id: String,
    pub qualifiers: Vec<&'static str>,
    /// `(var, property)`, the property is empty if the key is the element itself.
    pub keys: Vec<(String, String)>,
    /// `(side, var)`
    pub nodes: Vec<(&'static str, String)>,
    /// `(side, var, from, to)`
    pub edges: Vec<(&'static str, String, String, String)>,
    /// `(var, type name)`
    pub types: Vec<(String, String)>,
    /// `(var, label)`
    pub labels: Vec<(String, String)>,
    /// Anonymous variables
    pub anonymous: Vec<String>,
    /// `(side, condition number, operator)`
    pub conditions: Vec<(&'static str, u32, String)>,
    /// `(side, condition number, position, kind, var, value)` where kind is `var`, `prop` or
    /// `const`.
    pub terms: Vec<(&'static str, u32, u32, &'static str, String, String)>,
}

impl ConstraintDef {
    /// Name of a variable. Canonical names only depend on the position of the variable so that
    /// two constraints differing only by the names of their variables produce the same text.
    fn var_name(&self, v: VarId, canonical: bool) -> String {
        let info = &self.vars[v as usize];
        if info.anonymous {
            format!("_{}", v)
        } else if canonical {
            format!("v{}", v)
        } else {
            info.name.clone()
        }
    }

    fn operand_text(&self, o: &Operand, canonical: bool) -> String {
        match o {
            Operand::Var(v) => self.var_name(*v, canonical),
            Operand::Prop(v, p) => format!("{}.{}", self.var_name(*v, canonical), p),
            Operand::Const(c) => c.clone(),
        }
    }

    fn term_text(&self, t: &Term, canonical: bool) -> String {
        match t {
            Term::Simple(o) => self.operand_text(o, canonical),
            Term::Dist(a, b) => format!(
                "DIST({}, {})",
                self.operand_text(a, canonical),
                self.operand_text(b, canonical)
            ),
        }
    }

    /// Iterates over the properties used by the constraint with the variable they belong to.
    fn used_props(&self) -> impl Iterator<Item = (VarId, &String)> {
        fn operand(o: &Operand) -> Option<(VarId, &String)> {
            match o {
                Operand::Prop(v, p) => Some((*v, p)),
                _ => None,
            }
        }
        fn term(t: &Term) -> [Option<&Operand>; 2] {
            match t {
                Term::Simple(o) => [Some(o), None],
                Term::Dist(a, b) => [Some(a), Some(b)],
            }
        }
        self.keys.iter().filter_map(operand).chain(
            self.lhs
                .conditions
                .iter()
                .chain(self.rhs.conditions.iter())
                .flat_map(|c| term(&c.left).into_iter().chain(term(&c.right)))
                .flatten()
                .filter_map(operand),
        )
    }
}

impl Constraint {
    /// Name of the type bound to the slot of a variable, if any.
    fn type_name<'a>(&self, g: &'a PropertyGraph, v: VarId) -> Option<&'a str> {
        let info = &self.def.vars[v as usize];
        let slot = info.slot? as usize;
        let weight = if info.edge {
            g.graph.edge_weight(self.edges[slot])
        } else {
            g.graph.node_weight(self.nodes[slot])
        };
        Some(weight.map(|w| w.name.as_str()).unwrap_or("?"))
    }

    /// Writes a variable as it appears in a match. The type and labels are only written the
    /// first time the variable occurs.
    fn write_elem(
        &self,
        out: &mut String,
        g: &PropertyGraph,
        v: VarId,
        canonical: bool,
        seen: &mut [bool],
    ) {
        let info = &self.def.vars[v as usize];
        if !info.anonymous {
            out.push_str(&self.def.var_name(v, canonical));
        }
        if !seen[v as usize] {
            seen[v as usize] = true;
            if let Some(name) = self.type_name(g, v) {
                out.push(':');
                out.push_str(name);
                for label in info.labels.iter() {
                    out.push('&');
                    out.push_str(label);
                }
            }
        }
    }

    fn write_atoms(
        &self,
        out: &mut String,
        g: &PropertyGraph,
        side: &Side,
        canonical: bool,
        seen: &mut [bool],
    ) {
        for (i, atom) in side.atoms.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            match atom {
                Atom::Node(v) => {
                    out.push('(');
                    self.write_elem(out, g, *v, canonical, seen);
                    out.push(')');
                }
                Atom::Edge { var, from, to } => {
                    out.push('(');
                    self.write_elem(out, g, *from, canonical, seen);
                    out.push_str(")-[");
                    self.write_elem(out, g, *var, canonical, seen);
                    out.push_str("]->(");
                    self.write_elem(out, g, *to, canonical, seen);
                    out.push(')');
                }
            }
        }
    }

    fn write_conditions(&self, out: &mut String, side: &Side, canonical: bool) {
        for (i, c) in side.conditions.iter().enumerate() {
            out.push_str(if i == 0 { " WHERE " } else { " AND " });
            let _ = write!(
                out,
                "{} {} {}",
                self.def.term_text(&c.left, canonical),
                c.op,
                self.def.term_text(&c.right, canonical)
            );
        }
    }

    /// Text of the constraint using the current names of the types in `g`. If `canonical` is
    /// set, variables are renamed so that the text identifies the constraint: it is the
    /// identifier used in Datalog. The text can be parsed back.
    pub fn text(&self, g: &PropertyGraph, canonical: bool) -> String {
        let def = &*self.def;
        let mut out = String::from("FOR ");
        let mut seen = vec![false; def.vars.len()];
        self.write_atoms(&mut out, g, &def.lhs, canonical, &mut seen);
        self.write_conditions(&mut out, &def.lhs, canonical);
        for (flag, name) in [
            (def.exclusive, " EXCLUSIVE"),
            (def.mandatory, " MANDATORY"),
            (def.singleton, " SINGLETON"),
        ] {
            if flag {
                out.push_str(name);
            }
        }
        for (i, key) in def.keys.iter().enumerate() {
            out.push_str(if i == 0 { " " } else { ", " });
            out.push_str(&def.operand_text(key, canonical));
        }
        if !def.rhs.atoms.is_empty() {
            out.push_str(" WITHIN ");
            self.write_atoms(&mut out, g, &def.rhs, canonical, &mut seen);
        }
        self.write_conditions(&mut out, &def.rhs, canonical);
        out
    }

    /// Checks that the constraint makes sense in the given schema: every type it refers to
    /// exists with the properties and labels the constraint uses, and the typed endpoints of its
    /// edge matches agree with the schema. Does not allocate.
    pub fn validate(&self, g: &PropertyGraph) -> bool {
        let def = &*self.def;
        for (req, id) in def.node_slots.iter().zip(self.nodes.iter()) {
            match g.graph.node_weight(*id) {
                Some(w) => {
                    if !req.props.iter().all(|p| w.map.contains_key(p)) {
                        return false;
                    }
                }
                None => return false,
            }
            for label in req.labels.iter() {
                match g.vertex_label.get_id(label) {
                    Some(l) if g.vertex_label.has_label(id, *l) => (),
                    _ => return false,
                }
            }
        }
        for (req, id) in def.edge_slots.iter().zip(self.edges.iter()) {
            match g.graph.edge_weight(*id) {
                Some(w) => {
                    if !req.props.iter().all(|p| w.map.contains_key(p)) {
                        return false;
                    }
                }
                None => return false,
            }
            for label in req.labels.iter() {
                match g.edge_label.get_id(label) {
                    Some(l) if g.edge_label.has_label(id, *l) => (),
                    _ => return false,
                }
            }
        }
        for atom in def.lhs.atoms.iter().chain(def.rhs.atoms.iter()) {
            if let Atom::Edge { var, from, to } = atom {
                if let Some(slot) = def.vars[*var as usize].slot {
                    // The edge exists: checked above.
                    let (src, dst) = g.graph.edge_endpoints(self.edges[slot as usize]).unwrap();
                    for (end, expected) in [(from, src), (to, dst)] {
                        if let Some(s) = def.vars[*end as usize].slot {
                            if self.nodes[s as usize] != expected {
                                return false;
                            }
                        }
                    }
                }
            }
        }
        true
    }

    /// True if the constraint refers to the given node type.
    pub fn uses_node(&self, id: NodeIndex) -> bool {
        self.nodes.contains(&id)
    }

    /// True if the constraint refers to the given edge type.
    pub fn uses_edge(&self, id: EdgeIndex) -> bool {
        self.edges.contains(&id)
    }

    /// True if the constraint uses the given property of the given node type.
    pub fn uses_node_prop(&self, id: NodeIndex, prop: &str) -> bool {
        self.nodes
            .iter()
            .zip(self.def.node_slots.iter())
            .any(|(n, req)| *n == id && req.props.iter().any(|p| p == prop))
    }

    /// True if the constraint uses the given property of the given edge type.
    pub fn uses_edge_prop(&self, id: EdgeIndex, prop: &str) -> bool {
        self.edges
            .iter()
            .zip(self.def.edge_slots.iter())
            .any(|(e, req)| *e == id && req.props.iter().any(|p| p == prop))
    }

    /// True if the constraint requires the given label on the given node type.
    pub fn uses_node_label(&self, id: NodeIndex, label: &str) -> bool {
        self.nodes
            .iter()
            .zip(self.def.node_slots.iter())
            .any(|(n, req)| *n == id && req.labels.iter().any(|l| l == label))
    }

    /// True if the constraint requires the given label on the given edge type.
    pub fn uses_edge_label(&self, id: EdgeIndex, label: &str) -> bool {
        self.edges
            .iter()
            .zip(self.def.edge_slots.iter())
            .any(|(e, req)| *e == id && req.labels.iter().any(|l| l == label))
    }

    /// Replaces an edge index (an edge that is moved gets a new index).
    pub fn replace_edge(&mut self, old: EdgeIndex, new: EdgeIndex) {
        for e in self.edges.iter_mut() {
            if *e == old {
                *e = new;
            }
        }
    }

    /// True if both constraints are bound to the same types of `g` and state the same thing.
    pub fn same_as(&self, other: &Constraint, g: &PropertyGraph) -> bool {
        self.nodes == other.nodes
            && self.edges == other.edges
            && (Arc::ptr_eq(&self.def, &other.def) || self.text(g, true) == other.text(g, true))
    }

    /// Unbinds the constraint from its schema: slots are given by the current names of the types.
    pub fn to_pool_entry(&self, g: &PropertyGraph) -> PoolEntry {
        PoolEntry {
            def: self.def.clone(),
            nodes: self
                .nodes
                .iter()
                .map(|n| g.graph.node_weight(*n).map(|w| w.name.clone()).unwrap_or_default())
                .collect(),
            edges: self
                .edges
                .iter()
                .map(|e| g.graph.edge_weight(*e).map(|w| w.name.clone()).unwrap_or_default())
                .collect(),
        }
    }

    /// The Datalog facts describing the constraint, using the current names of the types in `g`.
    pub fn facts(&self, g: &PropertyGraph) -> ConstraintFacts {
        let def = &*self.def;
        let name = |v: VarId| def.var_name(v, true);
        let mut facts = ConstraintFacts {
            id: self.text(g, true),
            ..Default::default()
        };
        for (flag, q) in [
            (def.exclusive, "EXCLUSIVE"),
            (def.mandatory, "MANDATORY"),
            (def.singleton, "SINGLETON"),
        ] {
            if flag {
                facts.qualifiers.push(q);
            }
        }
        for key in def.keys.iter() {
            match key {
                Operand::Var(v) => facts.keys.push((name(*v), String::new())),
                Operand::Prop(v, p) => facts.keys.push((name(*v), p.clone())),
                Operand::Const(_) => (),
            }
        }
        for (v, info) in def.vars.iter().enumerate() {
            let v = v as VarId;
            if let Some(t) = self.type_name(g, v) {
                facts.types.push((name(v), t.to_string()));
            }
            for label in info.labels.iter() {
                facts.labels.push((name(v), label.clone()));
            }
            if info.anonymous {
                facts.anonymous.push(name(v));
            }
        }
        for (side_name, side) in [(SIDE_FOR, &def.lhs), (SIDE_WITHIN, &def.rhs)] {
            let mut nodes: Vec<VarId> = Vec::new();
            let mut add_node = |v: VarId| {
                if !nodes.contains(&v) {
                    nodes.push(v);
                }
            };
            for atom in side.atoms.iter() {
                match atom {
                    Atom::Node(v) => add_node(*v),
                    Atom::Edge { var, from, to } => {
                        add_node(*from);
                        add_node(*to);
                        facts
                            .edges
                            .push((side_name, name(*var), name(*from), name(*to)));
                    }
                }
            }
            facts
                .nodes
                .extend(nodes.into_iter().map(|v| (side_name, name(v))));
            for (n, c) in side.conditions.iter().enumerate() {
                let n = n as u32;
                let prefix = |t: &Term| if matches!(t, Term::Dist(..)) { "DIST" } else { "" };
                facts.conditions.push((
                    side_name,
                    n,
                    format!("{}{}{}", prefix(&c.left), c.op, prefix(&c.right)),
                ));
                let mut pos = 0;
                for term in [&c.left, &c.right] {
                    let operands: [Option<&Operand>; 2] = match term {
                        Term::Simple(o) => [Some(o), None],
                        Term::Dist(a, b) => [Some(a), Some(b)],
                    };
                    for o in operands.into_iter().flatten() {
                        let (kind, var, value) = match o {
                            Operand::Var(v) => ("var", name(*v), String::new()),
                            Operand::Prop(v, p) => ("prop", name(*v), p.clone()),
                            Operand::Const(c) => ("const", String::new(), c.clone()),
                        };
                        facts.terms.push((side_name, n, pos, kind, var, value));
                        pos += 1;
                    }
                }
            }
        }
        facts
    }
}

/// Builds a constraint definition from the elements found by the parser. Matches must be added
/// before the keys and conditions that refer to their variables.
#[derive(Debug, Default)]
pub struct ConstraintBuilder {
    def: ConstraintDef,
    var_ids: HashMap<String, VarId>,
    node_types: Vec<String>,
    edge_types: Vec<String>,
}

impl ConstraintBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_qualifiers(&mut self, exclusive: bool, mandatory: bool, singleton: bool) {
        self.def.exclusive = exclusive;
        self.def.mandatory = mandatory;
        self.def.singleton = singleton;
    }

    /// Declares the occurrence of a variable in a match. `name` is `None` for an anonymous
    /// variable, `ty` is the name of its type if given.
    pub fn var(
        &mut self,
        name: Option<&str>,
        edge: bool,
        ty: Option<&str>,
        labels: Vec<String>,
    ) -> Result<VarId, ConstraintError> {
        let id = match name.and_then(|n| self.var_ids.get(n)) {
            Some(id) => *id,
            None => {
                let id = self.def.vars.len() as VarId;
                self.def.vars.push(VarInfo {
                    name: name.unwrap_or_default().to_string(),
                    anonymous: name.is_none(),
                    edge,
                    slot: None,
                    labels: Vec::new(),
                });
                if let Some(name) = name {
                    self.var_ids.insert(name.to_string(), id);
                }
                id
            }
        };
        if self.def.vars[id as usize].edge != edge {
            return Err(ConstraintError::KindMismatch(
                name.unwrap_or_default().to_string(),
            ));
        }
        if let Some(ty) = ty {
            let types = if edge {
                &mut self.edge_types
            } else {
                &mut self.node_types
            };
            let slot = match types.iter().position(|t| t == ty) {
                Some(slot) => slot,
                None => {
                    types.push(ty.to_string());
                    types.len() - 1
                }
            } as SlotId;
            match self.def.vars[id as usize].slot {
                Some(old) if old != slot => {
                    return Err(ConstraintError::TypeMismatch(
                        name.unwrap_or_default().to_string(),
                        types[old as usize].clone(),
                        ty.to_string(),
                    ));
                }
                _ => self.def.vars[id as usize].slot = Some(slot),
            }
        }
        let info = &mut self.def.vars[id as usize];
        for label in labels {
            if !info.labels.contains(&label) {
                info.labels.push(label);
            }
        }
        Ok(id)
    }

    /// Finds a variable bound by a match.
    pub fn lookup(&self, name: &str) -> Result<VarId, ConstraintError> {
        self.var_ids
            .get(name)
            .copied()
            .ok_or_else(|| ConstraintError::UnknownVariable(name.to_string()))
    }

    /// Adds a match to the `FOR` side (`within` false) or to the `WITHIN` side.
    pub fn add_atom(&mut self, within: bool, atom: Atom) {
        self.side(within).atoms.push(atom);
    }

    /// Adds a condition to the `FOR` side (`within` false) or to the `WITHIN` side.
    pub fn add_condition(&mut self, within: bool, condition: Condition) {
        self.side(within).conditions.push(condition);
    }

    pub fn add_key(&mut self, key: Operand) {
        self.def.keys.push(key);
    }

    fn side(&mut self, within: bool) -> &mut Side {
        if within {
            &mut self.def.rhs
        } else {
            &mut self.def.lhs
        }
    }

    /// Computes what is required from each type and returns the definition with the names of
    /// the node types and of the edge types of its slots.
    pub fn finish(mut self) -> PoolEntry {
        let mut node_slots = vec![SlotReq::default(); self.node_types.len()];
        let mut edge_slots = vec![SlotReq::default(); self.edge_types.len()];
        fn req<'a>(
            def: &ConstraintDef,
            v: VarId,
            node_slots: &'a mut [SlotReq],
            edge_slots: &'a mut [SlotReq],
        ) -> Option<&'a mut SlotReq> {
            let info = &def.vars[v as usize];
            let slots = if info.edge { edge_slots } else { node_slots };
            info.slot.map(|slot| &mut slots[slot as usize])
        }
        for (v, info) in self.def.vars.iter().enumerate() {
            if let Some(req) = req(&self.def, v as VarId, &mut node_slots, &mut edge_slots) {
                for label in info.labels.iter() {
                    if !req.labels.contains(label) {
                        req.labels.push(label.clone());
                    }
                }
            }
        }
        for (v, prop) in self.def.used_props() {
            if let Some(req) = req(&self.def, v, &mut node_slots, &mut edge_slots) {
                if !req.props.contains(prop) {
                    req.props.push(prop.clone());
                }
            }
        }
        self.def.node_slots = node_slots;
        self.def.edge_slots = edge_slots;
        PoolEntry {
            def: Arc::new(self.def),
            nodes: self.node_types,
            edges: self.edge_types,
        }
    }
}

impl PoolEntry {
    /// Binds the constraint using the given functions to find a type from its name.
    pub fn bind<N, E>(&self, node: N, edge: E) -> Result<Constraint, ConstraintError>
    where
        N: Fn(&str) -> Option<NodeIndex>,
        E: Fn(&str) -> Option<EdgeIndex>,
    {
        Ok(Constraint {
            def: self.def.clone(),
            nodes: self
                .nodes
                .iter()
                .map(|n| node(n).ok_or_else(|| ConstraintError::UnknownNodeType(n.clone())))
                .collect::<Result<_, _>>()?,
            edges: self
                .edges
                .iter()
                .map(|e| edge(e).ok_or_else(|| ConstraintError::UnknownEdgeType(e.clone())))
                .collect::<Result<_, _>>()?,
        })
    }
}
