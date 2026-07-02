use std::collections::HashMap;
use std::fmt::Display;
use docopt::Docopt;
use serde::Deserialize;
use transproof::parsing::PropertyGraphParser;
use transproof::transformation::souffle::{RelationNames, TARGET_RELATION_NAMES, INPUT_RELATION_NAMES};
use transproof::property_graph::PropertyGraph;

use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeReferences, NodeRef};


fn fill_relation<E, I, F, O>(relation_name: &str, elements: I, to_tuple: F)
where
    I: Iterator<Item = E>,
    F: Fn(E) -> O,
    O: Display,
{
    for element in elements {
        print!("{}(",relation_name);
        unsafe {
            print!("{}", to_tuple(element));
        }
        println!(").");
    }
}

fn encode_graph(graph: &PropertyGraph, relation_names: &RelationNames<'static>) {
    let vid_to_name: HashMap<u32, &str> = graph
        .graph
        .node_references()
        .map(|(index, props)| (index.id().index() as u32, props.name.as_str()))
        .collect();
    let lvid_to_label: HashMap<u32, &str> = graph
        .vertex_label
        .labels()
        .map(|&id| (id, graph.vertex_label.get_label(id).unwrap().as_str()))
        .collect();
    let eid_to_name: HashMap<u32, &str> = graph
        .graph
        .edge_references()
        .map(|eref| (eref.id().index() as u32, eref.weight().name.as_str()))
        .collect();
    let leid_to_label: HashMap<u32, &str> = graph
        .edge_label
        .labels()
        .map(|&id| (id, graph.edge_label.get_label(id).unwrap().as_str()))
        .collect();
    fill_relation(
        relation_names.vertex_label,
        lvid_to_label.values(),
        |name| {
            format!("\"{}\"",name)
        },
    );
    fill_relation(
        relation_names.vertex,
        graph.graph.node_references(),
        |(_, prop)| {
            format!("\"{}\"",prop.name)
        },
    );
    fill_relation(
        relation_names.vertex_has_label,
        graph.graph.node_indices().flat_map(|id| {
            std::iter::repeat(vid_to_name.get(&(id.index() as u32)).unwrap()).zip(
                graph
                    .vertex_label
                    .element_labels(&id)
                    .map(|&id| lvid_to_label.get(&id).unwrap()),
            )
        }),
        |(vertex, label)| {
            // print!("{}, {}",vertex.index(),label);
            format!("\"{}\", \"{}\"", vertex, label)
        },
    );
    fill_relation(
        relation_names.vertex_property,
        graph.graph.node_indices().flat_map(|n| {
            let weight = graph.graph.node_weight(n).unwrap();
            std::iter::repeat(vid_to_name.get(&(n.index() as u32)).unwrap())
                .zip(weight.map.iter())
                .map(|(n, pair)| (n, pair.0, pair.1))
        }),
        |data| {
            format!("\"{}\", \"{}\", \"{}\"", data.0, data.1, data.2)
        },
    );
    fill_relation(
        relation_names.edge_label,
        leid_to_label.values(),
        |name| {
            format!("\"{}\"",name)
        },
    );
    fill_relation(
        relation_names.edge,
        graph.graph.edge_references().map(|eref| {
            (
                eid_to_name.get(&(eref.id().index() as u32)).unwrap(),
                vid_to_name.get(&(eref.source().index() as u32)).unwrap(),
                vid_to_name.get(&(eref.target().index() as u32)).unwrap(),
            )
        }),
        |(edge, source, target)| {
            format!("\"{}\", \"{}\", \"{}\"", edge, source, target)
        },
    );
    fill_relation(
        relation_names.edge_has_label,
        graph.graph.edge_indices().flat_map(|id| {
            std::iter::repeat(eid_to_name.get(&(id.index() as u32)).unwrap()).zip(
                graph
                    .edge_label
                    .element_labels(&id)
                    .map(|id| leid_to_label.get(id).unwrap()),
            )
        }),
        |(edge, label)| {
            format!("\"{}\", \"{}\"", edge, label)
        },
    );
    fill_relation(
        relation_names.edge_property,
        graph.graph.edge_indices().flat_map(|e| {
            let weight = graph.graph.edge_weight(e).unwrap();
            std::iter::repeat(eid_to_name.get(&(e.index() as u32)).unwrap())
                .zip(weight.map.iter())
                .map(|(n, pair)| (n, pair.0, pair.1))
        }),
        |data| {
            format!("\"{}\", \"{}\", \"{}\"", data.0, data.1, data.2)
        },
    );
}

pub fn encode_input_graph(graph: &PropertyGraph) {
    encode_graph(graph, &INPUT_RELATION_NAMES);
}

pub fn encode_target_graph(graph: &PropertyGraph) {
    encode_graph(graph, &TARGET_RELATION_NAMES);
}

const USAGE: &str = "
Converts source and target from pgschema to souffle.

Usage:
    serialize_graph <source> <target>
";

#[derive(Debug, Deserialize, Clone)]
struct Args {
    arg_source: String,
    arg_target: String
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let parser = PropertyGraphParser;


    let source = {
        let source = std::fs::read_to_string(&args.arg_source).unwrap();
        let mut data = parser.convert_text(&source);
        data.pop().unwrap()
    };

    let target = {
        let target = std::fs::read_to_string(&args.arg_target).unwrap();
        let mut data = parser.convert_text(&target);
        data.pop().unwrap()
    };
    encode_input_graph(&source);
    encode_target_graph(&target);
}
