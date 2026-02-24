use std::collections::HashMap;

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use petgraph::graph::NodeIndex;

use crate::property_graph::{Properties, PropertyGraph};

#[derive(Parser)]
#[grammar = "parsing/PropertyGraph.pest"]
pub struct PropertyGraphParser;

impl PropertyGraphParser {
    pub fn convert_text(&self, input: &str) -> Vec<PropertyGraph> {
        let mut res : Vec<PropertyGraph> = PropertyGraphParser::parse(Rule::schemas, input)
            .unwrap()
            .next()
            .unwrap()
            .into_inner()
            .map(|v| self.build_graph(v))
            .collect();
        res.pop();
        res
    }

    pub fn build_graph(&self, v: Pair<'_, Rule>) -> PropertyGraph {
        v.as_rule();
        let mut graph = PropertyGraph::default();
        let mut names = HashMap::new();
        self.handle_result(v, &mut graph, &mut names);
        graph
    }

    fn extract_labels(&self, v: Pair<'_, Rule>, lst: &mut Vec<String>) {
        match v.as_rule() {
            Rule::labelSpecSet => v.into_inner().for_each(|i| self.extract_labels(i, lst)),
            Rule::labelSpec => v.into_inner().for_each(|i| self.extract_labels(i, lst)),
            Rule::label => lst.push(v.as_str().to_string()),
            _ => (),
        }
    }

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

    fn handle_result(
        &self,
        v: Pair<'_, Rule>,
        graph: &mut PropertyGraph,
        names: &mut HashMap<String, NodeIndex>,
    ) {
        match v.as_rule() {
            Rule::createGraphType => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names)),
            Rule::graphType => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names)),
            Rule::graphTypeElements => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names)),
            Rule::elementTypes => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names)),
            Rule::elementType => v
                .into_inner()
                .for_each(|i| self.handle_result(i, graph, names)),
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
                    name: name,
                    map: props,
                };
                let edge = graph.graph.add_edge(*first, *end, data);
                let labels: Vec<_> = labels
                    .into_iter()
                    .map(|label| graph.edge_label.add_label(label))
                    .collect();
                for label in labels {
                    graph.edge_label.add_label_mapping(&edge, label).unwrap();
                }
            }
            _ => (),
        }
    }
}

#[cfg(test)]
mod test {
    use super::PropertyGraphParser;

    #[test]
    fn smoke_test() {
        let text = "CREATE GRAPH TYPE fraudGraphType {
( personType : Person { name STRING , birthday DATE }) ,
( customerType : Person & Customer { name STRING , since DATE }) ,
( suspiciousType : Suspicious { reason STRING }) ,
( : customerType )
-[ friendType : Knows & Likes {time INT} ] ->
( : customerType ),
( : suspiciousType )
-[ aliasType {frequency INT} ] ->
( : suspiciousType )
}";
        let parser = PropertyGraphParser;
        let results = parser.convert_text(text);
        let g = results.get(0).unwrap();
        println!("{}", g);
        println!("{}", results.len());
    }
}
