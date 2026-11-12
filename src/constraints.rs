use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use petgraph::{graph::EdgeIndex, graph::NodeIndex};

use crate::property_graph::PropertyGraph;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum Type {
    Node { id: NodeIndex, name: String },
    Edge { id: EdgeIndex, name: String },
}

#[derive(Debug, Clone, Default)]
pub struct Constraint {
    pub text: String,
    pub vars: HashMap<String, Option<Type>>,
    pub types: HashSet<Type>,
    pub props: HashMap<Type, HashSet<String>>,
}

impl Constraint {
    pub fn new() -> Self {
        Constraint {
            text: "FOR".to_string(),
            vars: HashMap::new(),
            types: HashSet::new(),
            props: HashMap::new(),
        }
    }

    pub fn add_var(&mut self, var: String, var_type: Option<Type>) {
        if let Some(None) = self.vars.get(&var) {
            self.vars.insert(var, var_type.clone());
        } else if !self.vars.contains_key(&var) {
            self.vars.insert(var, var_type.clone());
        }
        if let Some(type_) = var_type {
            self.types.insert(type_);
        }
    }

    pub fn add_prop(&mut self, var: String, prop: String) {
        let type_ = self.vars.get(&var);
        if let Some(Some(p_type)) = type_ {
            self.types.insert(p_type.clone());
            self.props.entry(p_type.clone()).or_default().insert(prop);
        }
    }

    pub fn append_text(&mut self, text: &str) {
        self.text += " ";
        self.text += text;
    }

    pub fn validate(&self, g: &PropertyGraph) -> bool {
        for t in self.types.iter() {
            let props = self.props.get(t);
            let (weights, name) = match t {
                Type::Node { id, name } => (g.graph.node_weight(*id), name),
                Type::Edge { id, name } => (g.graph.edge_weight(*id), name),
            };
            if let Some(p) = weights {
                if p.name == *name {
                    for prop in props.iter().flat_map(|v| v.iter()) {
                        if !p.map.contains_key(prop) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //FIXME: Need to adapt to relabelling
        write!(f, "{}", self.text)
    }
}
