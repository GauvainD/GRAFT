//! Edit operation types shared between the Souffle FFI and the graph transformation engine.
//!
//! This module defines:
//! - [`OperationName`] — a lightweight, argument-free tag identifying which kind of operation is
//!   being described (used in the automaton before arguments are known).
//! - [`Operation`] — the full, argument-carrying enum that is applied to a [`crate::property_graph::PropertyGraph`].
//! - [`souffle`] — the low-level FFI bridge that drives compiled Souffle programs.

use crate::graph_transformation::GraphTransformation;
use crate::property_graph::PropertyGraph;
use crate::transformation_automaton::TransformGeneratorGraph;
use lazy_static::lazy_static;
use souffle::generate_operation_automaton;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

use self::souffle::Program;

/// Low-level FFI bridge to compiled Souffle programs.
pub mod souffle;

/// Argument-free tag identifying which kind of edit operation is being described.
///
/// `OperationName` is cheaper to store and compare than a full [`Operation`] because it carries
/// no `String` arguments. It is used inside the automaton to detect commutative pairs and to
/// decode Souffle record indices before argument strings are available.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum OperationName {
    /// Add a label declaration for a vertex type.
    AddVertexLabel,
    /// Remove a label declaration from a vertex type.
    RemoveVertexLabel,
    /// Add a label declaration for an edge type.
    AddEdgeLabel,
    /// Remove a label declaration from an edge type.
    RemoveEdgeLabel,
    /// Add a new vertex type to the schema.
    AddVertex,
    /// Remove an existing vertex type from the schema.
    RemoveVertex,
    /// Add a new edge type between two vertex types.
    AddEdge,
    /// Remove an existing edge type from the schema.
    RemoveEdge,
    /// Add a property key to a vertex type.
    AddVertexProperty,
    /// Remove a property key from a vertex type.
    RemoveVertexProperty,
    /// Add a property key to an edge type.
    AddEdgeProperty,
    /// Remove a property key from an edge type.
    RemoveEdgeProperty,
    /// Rename a vertex type.
    RenameVertex,
    /// Rename an edge type.
    RenameEdge,
    /// Move an edge type's target endpoint to a different vertex type.
    MoveEdgeTarget,
    /// Move an edge type's source endpoint to a different vertex type.
    MoveEdgeSource,
}

impl From<Operation> for OperationName {
    fn from(value: Operation) -> Self {
        match value {
            Operation::AddVertexLabel(_, _) => Self::AddVertexLabel,
            Operation::RemoveVertexLabel(_, _) => Self::RemoveVertexLabel,
            Operation::AddEdgeLabel(_, _) => Self::AddEdgeLabel,
            Operation::RemoveEdgeLabel(_, _) => Self::RemoveEdgeLabel,
            Operation::AddVertex(_) => Self::AddVertex,
            Operation::RemoveVertex(_) => Self::RemoveVertex,
            Operation::AddEdge(_, _, _) => Self::AddEdge,
            Operation::RemoveEdge(_) => Self::RemoveEdge,
            Operation::AddVertexProperty(_, _, _) => Self::AddVertexProperty,
            Operation::RemoveVertexProperty(_, _) => Self::RemoveVertexProperty,
            Operation::AddEdgeProperty(_, _, _) => Self::AddEdgeProperty,
            Operation::RemoveEdgeProperty(_, _) => Self::RemoveEdgeProperty,
            Operation::RenameVertex(_, _) => Self::RenameVertex,
            Operation::RenameEdge(_, _) => Self::RenameEdge,
            Operation::MoveEdgeTarget(_, _) => Self::MoveEdgeTarget,
            Operation::MoveEdgeSource(_, _) => Self::MoveEdgeSource,
        }
    }
}

impl OperationName {
    /// Returns the name of the operation as a string
    fn symbol<'a>(&'a self) -> &'a str {
        match self {
            OperationName::AddEdge => "AddEdge",
            OperationName::AddVertexLabel => "AddVertexLabel",
            OperationName::RemoveVertexLabel => "RemoveVertexLabel",
            OperationName::AddEdgeLabel => "AddEdgeLabel",
            OperationName::RemoveEdgeLabel => "RemoveEdgeLabel",
            OperationName::AddVertex => "AddVertex",
            OperationName::RemoveVertex => "RemoveVertex",
            OperationName::RemoveEdge => "RemoveEdge",
            OperationName::AddVertexProperty => "AddVertexProperty",
            OperationName::RemoveVertexProperty => "RemoveVertexProperty",
            OperationName::AddEdgeProperty => "AddEdgeProperty",
            OperationName::RemoveEdgeProperty => "RemoveEdgeProperty",
            OperationName::RenameVertex => "RenameVertex",
            OperationName::RenameEdge => "RenameEdge",
            OperationName::MoveEdgeTarget => "MoveEdgeTarget",
            OperationName::MoveEdgeSource => "MoveEdgeSource",
        }
    }

    /// Arity of the edit operation
    fn arity(&self) -> u32 {
        match self {
            OperationName::AddVertexLabel => 2,
            OperationName::RemoveVertexLabel => 2,
            OperationName::AddEdgeLabel => 2,
            OperationName::RemoveEdgeLabel => 2,
            OperationName::AddVertex => 1,
            OperationName::RemoveVertex => 1,
            OperationName::AddEdge => 3,
            OperationName::RemoveEdge => 1,
            OperationName::AddVertexProperty => 3,
            OperationName::RemoveVertexProperty => 2,
            OperationName::AddEdgeProperty => 3,
            OperationName::RemoveEdgeProperty => 2,
            OperationName::RenameVertex => 2,
            OperationName::RenameEdge => 2,
            OperationName::MoveEdgeTarget => 2,
            OperationName::MoveEdgeSource => 2,
        }
    }
}

lazy_static! {
    /// Souffle, by default, assigns an index to each record by alphabetical order.
    static ref OPERATION_ORDER: Vec<OperationName> = {
        let mut names = vec![
            OperationName::AddVertex,
            OperationName::AddVertexLabel,
            OperationName::AddVertexProperty,
            OperationName::AddEdge,
            OperationName::AddEdgeLabel,
            OperationName::AddEdgeProperty,
            OperationName::MoveEdgeTarget,
            OperationName::MoveEdgeSource,
            OperationName::RenameVertex,
            OperationName::RenameEdge,
            OperationName::RemoveEdgeProperty,
            OperationName::RemoveEdgeLabel,
            OperationName::RemoveEdge,
            OperationName::RemoveVertexProperty,
            OperationName::RemoveVertexLabel,
            OperationName::RemoveVertex,
        ];
        names.sort_by(|name1, name2| name1.symbol().cmp(name2.symbol()));
        names
    };
}

/// Returns the name of the edit operation from its index
fn name_from_order(v: i32) -> Option<OperationName> {
    if 0 <= v && v < OPERATION_ORDER.len() as i32 {
        Some(OPERATION_ORDER[v as usize])
    } else {
        None
    }
}

/// A fully-instantiated edit operation that can be applied to a property graph schema.
///
/// Each variant carries the name(s) of the affected schema element(s) as `String` arguments.
/// The argument order follows the pattern used in the Souffle Datalog definitions.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Operation {
    /// `(vertex_name, label_name)` — assign a label to a vertex type.
    AddVertexLabel(String, String),
    /// `(vertex_name, label_name)` — remove a label from a vertex type.
    RemoveVertexLabel(String, String),
    /// `(edge_name, label_name)` — assign a label to an edge type.
    AddEdgeLabel(String, String),
    /// `(edge_name, label_name)` — remove a label from an edge type.
    RemoveEdgeLabel(String, String),
    /// `(vertex_name)` — introduce a new vertex type.
    AddVertex(String),
    /// `(vertex_name)` — delete an existing vertex type.
    RemoveVertex(String),
    /// `(edge_name, source_vertex_name, target_vertex_name)` — introduce a new edge type.
    AddEdge(String, String, String),
    /// `(edge_name)` — delete an existing edge type.
    RemoveEdge(String),
    /// `(vertex_name, property_key, property_value)` — add a property to a vertex type.
    AddVertexProperty(String, String, String),
    /// `(vertex_name, property_key)` — remove a property from a vertex type.
    RemoveVertexProperty(String, String),
    /// `(edge_name, property_key, property_value)` — add a property to an edge type.
    AddEdgeProperty(String, String, String),
    /// `(edge_name, property_key)` — remove a property from an edge type.
    RemoveEdgeProperty(String, String),
    /// `(old_name, new_name)` — rename a vertex type.
    RenameVertex(String, String),
    /// `(old_name, new_name)` — rename an edge type.
    RenameEdge(String, String),
    /// `(edge_name, new_target_vertex_name)` — redirect an edge type's target endpoint.
    MoveEdgeTarget(String, String),
    /// `(edge_name, new_source_vertex_name)` — redirect an edge type's source endpoint.
    MoveEdgeSource(String, String),
}

/// Generates transformations from a schema g and a program
pub fn transform_graph(
    program: Program,
    g: &PropertyGraph,
    target_graph: &Option<PropertyGraph>,
) -> Option<TransformGeneratorGraph> {
    if let Some(graph) = generate_operation_automaton(program, g, target_graph) {
        Some(TransformGeneratorGraph::new(graph, g))
    } else {
        None
    }
}
