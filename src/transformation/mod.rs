//! Structures used to interact with edit operations.

use crate::graph_transformation::GraphTransformation;
use crate::property_graph::PropertyGraph;
use crate::transformation_automaton::TransformGeneratorGraph;
use lazy_static::lazy_static;
use souffle::generate_operation_automaton;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

use self::souffle::Program;

pub mod souffle;

/// Used to represent the name of an edit operation without having its arguments yet.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum OperationName {
    AddVertexLabel,
    RemoveVertexLabel,
    AddEdgeLabel,
    RemoveEdgeLabel,
    AddVertex,
    RemoveVertex,
    AddEdge,
    RemoveEdge,
    AddVertexProperty,
    RemoveVertexProperty,
    AddEdgeProperty,
    RemoveEdgeProperty,
    RenameVertex,
    RenameEdge,
    MoveEdgeTarget,
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
    /// Returns the the name of the operation as a string
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

/// Represents an edit operation
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Operation {
    AddVertexLabel(String, String),
    RemoveVertexLabel(String, String),
    AddEdgeLabel(String, String),
    RemoveEdgeLabel(String, String),
    AddVertex(String),
    RemoveVertex(String),
    AddEdge(String, String, String),
    RemoveEdge(String),
    AddVertexProperty(String, String, String),
    RemoveVertexProperty(String, String),
    AddEdgeProperty(String, String, String),
    RemoveEdgeProperty(String, String),
    RenameVertex(String, String),
    RenameEdge(String, String),
    MoveEdgeTarget(String, String),
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
