//! Used to augment schemas by inserting random nodes.
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::prelude::*;
use docopt::Docopt;
use serde::Deserialize;
use transproof::parsing::PropertyGraphParser;
use transproof::transformation::souffle::{RelationNames, TARGET_RELATION_NAMES, INPUT_RELATION_NAMES};
use transproof::property_graph::{Properties, PropertyGraph};

use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeReferences, NodeRef};

use rand::Rng;

const USAGE: &str = "
Randomly adds a node with a random label and two random properties

Usage:
    add_nodes_random [options] <source> <target>
    add_nodes_random (--help | -h)

Options:
    -h, --help              Print this help message
    -i, --inc <inc>         Number of random nodes to add [default: 1]
    -n <num>                Number of schemas to output [default: 1]
    -s, --size <size>       Number of characters per random value [default: 10]
    --identical             Use the same label, properties and values in both schemas
";

/// Generate a random string of given length
fn random_string(n: usize) -> String {
    rand::thread_rng().sample_iter(rand::distr::Alphabetic).take(n).map(char::from).collect()
}

/// Insert a random node into both schemas. If identical, the same strings are used in both.
fn add_random_node(source: &mut PropertyGraph,  target: &mut PropertyGraph, size: usize, identical: bool) {
    let mut name= "".to_string();
    let mut label= "".to_string();
    let mut prop1= "".to_string();
    let mut prop2= "".to_string();
    let mut props= HashMap::new();
    let mut propstruct= Properties{
        name,
        map: props,
    };
    let mut first = true;
    for pg in [source, target] {
        if first || !identical {
            name = random_string(size);
            label = random_string(size);
            prop1 = random_string(size);
            prop2 = random_string(size);
            props = HashMap::new();
            props.insert(prop1, "string".to_string());
            props.insert(prop2, "string".to_string());
            propstruct = Properties {
                name,
                map: props
            };
            first = false;
        }

        let node = pg.graph.add_node(propstruct.clone());
        let labelid = pg.vertex_label.add_label(label.clone());
        pg.vertex_label.add_label_mapping(&node, labelid).unwrap();
    }
}

#[derive(Debug, Deserialize, Clone)]
struct Args {
    arg_source: String,
    arg_target: String,
    flag_i: usize,
    flag_n: usize,
    flag_s: usize,
    flag_identical: bool
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let parser = PropertyGraphParser;


    let mut source = {
        let source = std::fs::read_to_string(&args.arg_source).unwrap();
        let mut data = parser.convert_text(&source);
        data.pop().unwrap()
    };

    let mut target = {
        let target = std::fs::read_to_string(&args.arg_target).unwrap();
        let mut data = parser.convert_text(&target);
        data.pop().unwrap()
    };

    let target_name = &args.arg_target.split(".").take(1).next().unwrap();
    let source_name = &args.arg_source.split(".").take(1).next().unwrap();

    for i in 0..=args.flag_n {
        let increment = i * args.flag_i;
        {
            let mut file = File::create(format!("{source_name}-{increment}.pgschema")).unwrap();
            file.write_all(source.to_string().as_bytes()).unwrap();
        }
        {
            let mut file = File::create(format!("{target_name}-{increment}.pgschema")).unwrap();
            file.write_all(target.to_string().as_bytes()).unwrap();
        }
        for _ in 0..args.flag_i {
            add_random_node(&mut source, &mut target, args.flag_s, args.flag_identical);
        }
    }
}
