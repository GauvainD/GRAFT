//! Module dedicated to parsing the pgschema format for Property Graph Schemas
use std::collections::HashMap;

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use petgraph::graph::{EdgeIndex, NodeIndex};

use crate::{
    constraints::{Constraint, Type},
    property_graph::{Properties, PropertyGraph},
};

/// Collapses every run of whitespace (spaces, tabs, newlines) into a single space
fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Parser for the pgschema format
#[derive(Parser)]
#[grammar = "parsing/PropertyGraph.pest"]
pub struct PropertyGraphParser;

impl PropertyGraphParser {
    /// Converts a pgschema string to a PropertyGraph
    pub fn convert_text(&self, input: &str) -> Vec<PropertyGraph> {
        let mut res: Vec<PropertyGraph> = PropertyGraphParser::parse(Rule::schemas, input)
            .unwrap()
            .next()
            .unwrap()
            .into_inner()
            .map(|v| self.build_graph(v))
            .collect();
        res.pop();
        res
    }

    /// Builds the Property Graph from the result of parsing
    pub fn build_graph(&self, v: Pair<'_, Rule>) -> PropertyGraph {
        v.as_rule();
        let mut graph = PropertyGraph::default();
        let mut names = HashMap::new();
        let mut enames = HashMap::new();
        let mut consts = Vec::new();
        self.handle_result(v, &mut graph, &mut names, &mut enames, &mut consts);
        graph.constraints.extend(consts.into_iter().map(|v| {
            let mut c = Constraint::new();
            self.build_constraint(v, &mut c, &names, &enames);
            c
        }));
        graph
    }

    /// Extracts the labels of a node/edge
    fn extract_labels(&self, v: Pair<'_, Rule>, lst: &mut Vec<String>) {
        match v.as_rule() {
            Rule::labelSpecSet => v.into_inner().for_each(|i| self.extract_labels(i, lst)),
            Rule::labelSpec => v.into_inner().for_each(|i| self.extract_labels(i, lst)),
            Rule::label => lst.push(v.as_str().to_string()),
            _ => (),
        }
    }

    /// Extracts the properties of a node/edge
    fn extract_properties(&self, v: Pair<'_, Rule>, props: &mut HashMap<String, String>) {
        match v.as_rule() {
            Rule::propertySpec => v
                .into_inner()
                .for_each(|i| self.extract_properties(i, props)),
            Rule::properties => v
                .into_inner()
                .for_each(|i| self.extract_properties(i, props)),
            Rule::property => {
                let mut pairs = v.into_inner();
                let key = pairs.next().unwrap().as_str().to_string();
                let tpe = pairs.next().unwrap().as_str().to_string();
                props.insert(key, tpe);
            }
            _ => (),
        }
    }

    /// Extracts data for a node/edge (label and properties)
    fn extract_label_and_props(
        &self,
        v: Pair<'_, Rule>,
        labels: &mut Vec<String>,
        props: &mut HashMap<String, String>,
    ) -> bool {
        match v.as_rule() {
            Rule::labelPropertySpec => {
                for pair in v.into_inner() {
                    match pair.as_rule() {
                        Rule::labelSpecSet => self.extract_labels(pair, labels),
                        Rule::propertySpec => self.extract_properties(pair, props),
                        _ => (),
                    }
                }
                true
            }
            _ => false,
        }
    }

    fn build_constraint(&self, v: Pair<'_, Rule>, c: &mut Constraint, names: &HashMap<String, NodeIndex>, enames: &HashMap<String, EdgeIndex>) {
        match v.as_rule() {
            Rule::constraint => v.into_inner().for_each(|i| self.build_constraint(i, c, names, enames)),
            Rule::constraintMatch => v.into_inner().for_each(|i| self.build_constraint(i, c, names, enames)),
            Rule::constraintElem => v.into_inner().for_each(|i| self.build_constraint(i, c, names, enames)),
            Rule::constraintWithin => {
                c.append_text("WITHIN");
                v.into_inner().for_each(|i| self.build_constraint(i, c, names, enames));
            }
            Rule::constraintWhere => {
                c.append_text("WHERE");
                v.into_inner().for_each(|i| self.build_constraint(i, c, names, enames));
            }
            Rule::constraintProp => {
                let mut pairs = v.into_inner();
                let var = pairs.next().unwrap().as_str().to_string();
                let prop = pairs.next().unwrap().as_str().to_string();
                c.append_text(&format!("{}.{}", var, prop));
                c.add_prop(var, prop);
            }
            Rule::constraintVar => {
                let var = v.as_str().to_string();
                c.add_var(var, None);
            }
            Rule::edgeMatch => {
                let mut pairs = v.into_inner();
                let left = self.edge_node_text(pairs.next().unwrap(), c, names, enames);
                let edge = pairs.next().unwrap();
                let right = self.edge_node_text(pairs.next().unwrap(), c, names, enames);
                let edge = self.type_match_text(edge, c, true, names, enames);
                c.append_text(&format!("{}-[{}]->{}", left, edge, right));
            }
            Rule::edgeNodeMatch => {
                let text = self.edge_node_text(v, c, names, enames);
                c.append_text(&text);
            }
            Rule::edgeTypeMatch => {
                let text = self.type_match_text(v, c, false, names, enames);
                c.append_text(&text);
            }
            Rule::nodeMatch => {
                let mut pairs = v.into_inner();
                let var = pairs.next().unwrap().as_str().to_string();
                let typename = pairs.next().unwrap().as_str();
                c.append_text(&format!("({}:{})", var, typename));
                //FIXME: remove the unwrap
                let id = names.get(&typename.to_string()).unwrap();
                let var_type = Type::Node {
                    id: *id,
                    name: typename.to_string(),
                };
                c.add_var(var, Some(var_type));
            }
            _ => {
                c.append_text(&squash(v.as_str()));
            }
        }
    }

    /// Registers the variable of an `edgeTypeMatch` and returns its canonical text
    /// (`var`, `var:Type` or `var:Type&Labels`). `edge` tells whether the variable
    /// stands for an edge or a node.
    fn type_match_text(&self, v: Pair<'_, Rule>, c: &mut Constraint, edge: bool, names: &HashMap<String, NodeIndex>, enames: &HashMap<String, EdgeIndex>) -> String {
        let mut pairs = v.into_inner();
        let var = pairs.next().unwrap().as_str().to_string();
        let mut text = var.clone();
        let mut var_type = None;
        for pair in pairs {
            match pair.as_rule() {
                Rule::typeName => {
                    let typename = pair.as_str().to_string();
                    text.push(':');
                    text.push_str(&typename);
                    var_type = Some(if edge {
                        let id = enames.get(&typename).unwrap();
                        Type::Edge { id: *id, name: typename.to_string() }
                    } else {
                        let id = names.get(&typename).unwrap();
                        Type::Node { id: *id, name: typename.to_string() }
                    });
                }
                Rule::labelSpecSet => {
                    text.push_str(" & ");
                    text.extend(pair.as_str().split_whitespace());
                }
                _ => (),
            }
        }
        c.add_var(var, var_type);
        text
    }

    /// Registers the variable of an `edgeNodeMatch` (if any) and returns its canonical text.
    fn edge_node_text(&self, v: Pair<'_, Rule>, c: &mut Constraint, names: &HashMap<String, NodeIndex>, enames: &HashMap<String, EdgeIndex>) -> String {
        match v.into_inner().next() {
            Some(inner) => format!("({})", self.type_match_text(inner, c, false, names, enames)),
            None => "()".to_string(),
        }
    }

    /// From the results of parsing, populates the provided property graph and the map from node
    /// name to node index.
    fn handle_result<'a>(
        &self,
        v: Pair<'a, Rule>,
        graph: &mut PropertyGraph,
        names: &mut HashMap<String, NodeIndex>,
        enames: &mut HashMap<String, EdgeIndex>,
        consts: &mut Vec<Pair<'a, Rule>>
    ) {
        match v.as_rule() {
            Rule::createGraphType => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names, enames, consts)),
            Rule::graphType => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names, enames, consts)),
            Rule::graphTypeElements => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names, enames, consts)),
            Rule::elements => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names, enames, consts)),
            Rule::element => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names, enames, consts)),
            Rule::nodeType => {
                let mut pairs = v.into_inner();
                let name = pairs.next().unwrap().as_str().to_string();
                let mut labels = Vec::new();
                let mut props = HashMap::new();
                if let Some(pair) = pairs.peek() {
                    if self.extract_label_and_props(pair, &mut labels, &mut props) {
                        pairs.next().unwrap();
                    }
                }
                let data = Properties {
                    name: name.clone(),
                    map: props,
                };
                let node = graph.graph.add_node(data);
                names.insert(name, node);
                let labels: Vec<_> = labels
                    .into_iter()
                    .map(|label| graph.vertex_label.add_label(label))
                    .collect();
                for label in labels {
                    graph.vertex_label.add_label_mapping(&node, label).unwrap();
                }
            }
            Rule::edgeType => {
                let mut pairs = v.into_inner();
                let first_name = pairs
                    .next()
                    .unwrap()
                    .into_inner()
                    .next()
                    .unwrap()
                    .as_str()
                    .to_string();
                let first = names.get(&first_name).unwrap();
                let mut inner_pairs = pairs.next().unwrap().into_inner();
                let name = inner_pairs.next().unwrap().as_str().to_string();
                let mut labels = Vec::new();
                let mut props = HashMap::new();
                if let Some(pair) = inner_pairs.peek() {
                    if self.extract_label_and_props(pair, &mut labels, &mut props) {
                        inner_pairs.next().unwrap();
                    }
                }
                let last_name = pairs
                    .next()
                    .unwrap()
                    .into_inner()
                    .next()
                    .unwrap()
                    .as_str()
                    .to_string();
                let end = names.get(&last_name).unwrap();
                let data = Properties {
                    name: name.clone(),
                    map: props,
                };
                let edge = graph.graph.add_edge(*first, *end, data);
                enames.insert(name, edge);
                let labels: Vec<_> = labels
                    .into_iter()
                    .map(|label| graph.edge_label.add_label(label))
                    .collect();
                for label in labels {
                    graph.edge_label.add_label_mapping(&edge, label).unwrap();
                }
            }
            Rule::constraint => {
                consts.push(v);
            }
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn constraint_text(input: &str) -> String {
        let pairs = PropertyGraphParser::parse(Rule::schemas, input)
            .unwrap()
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap();
        fn find<'a>(p: Pair<'a, Rule>) -> Option<Pair<'a, Rule>> {
            if p.as_rule() == Rule::constraint {
                return Some(p);
            }
            p.into_inner().find_map(find)
        }
        let mut c = Constraint::new();
        PropertyGraphParser.build_constraint(find(pairs).unwrap(), &mut c);
        c.text
    }

    #[test]
    fn node_constraint_whitespace_insensitive() {
        let compact = "CREATE GRAPH TYPE T { (a: A {k INT}), FOR (x:A) MANDATORY x.k WHERE x.k = 1 }";
        let messy = "CREATE GRAPH TYPE T {\n (a: A {k INT}),\n FOR  ( x : A )\n  MANDATORY\n x . k\n WHERE   x.k  =\n 1 }";
        assert_eq!(constraint_text(compact), constraint_text(messy));
    }

    #[test]
    fn edge_constraint_whitespace_insensitive() {
        let compact = "CREATE GRAPH TYPE T { (a: A), (b: B), (:a) -[e: E]-> (:b), FOR (x)-[y:E]->(z) SINGLETON y WITHIN (w:A) }";
        let messy = "CREATE GRAPH TYPE T { (a: A), (b: B), (:a) -[e: E]-> (:b),\n FOR ( x ) - [ y : E ] - > ( z )\n SINGLETON\n y\n WITHIN ( w : A ) }";
        let text = constraint_text(compact);
        assert_eq!(text, constraint_text(messy));
        assert!(text.contains("(x)-[y:E]->(z)"), "{text}");
    }
}
