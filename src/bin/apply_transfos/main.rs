use std::{collections::HashMap, fs::read_to_string, io::stdin};

use docopt::Docopt;
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;
use serde::Deserialize;
use transproof::{constants::IDEMPOTENCE, graph_transformation::GraphTransformation, parsing::PropertyGraphParser, property_graph::{Properties, PropertyGraph}, similarity::jaccard_index, transformation::Operation};

#[derive(Parser)]
#[grammar = "src/bin/apply_transfos/transfos.pest"]
struct TransfosParser;

const USAGE: &str = "
Converts from datalog to pgschema

Usage:
    apply_transfos [options] <schema> <transfofile> <target>

Options:
    -i, --idempotency  Idempotency
";

#[derive(Debug, Deserialize, Clone)]
struct Args {
    arg_schema: String,
    arg_transfofile: String,
    arg_target: String,
    flag_idempotency: bool,
}

fn transform_and_print(g: &mut GraphTransformation, op: Operation, t: &PropertyGraph) {
    g.apply(&op);
    let sim = jaccard_index(&g.result, t);
    println!("{:?}", op);
    println!("{}", g);
    println!("sim: {}", sim);
    stdin().read_line(&mut String::new()).unwrap();
}

fn transform(g: &mut GraphTransformation, transfos: Pair<Rule>, t: &PropertyGraph) {
    match transfos.as_rule() {
        Rule::Main => {
            for rule in transfos.into_inner() {
                transform(g, rule, t);
            }
        },
        Rule::Transfo => {
            for rule in transfos.into_inner() {
                transform(g, rule, t);
            }
        }
        Rule::Edit => {
            for rule in transfos.into_inner() {
                transform(g, rule, t);
            }
        }
        Rule::AddVertex => {
            let name = transfos.into_inner().next().unwrap().as_str();
            let op = Operation::AddVertex(name.to_string());
            transform_and_print(g, op, t);
        }
        Rule::AddEdge => {
            let mut val_iter = transfos.into_inner();
            let name = val_iter.next().unwrap().as_str();
            let from = val_iter.next().unwrap().as_str();
            let to = val_iter.next().unwrap().as_str();
            let op = Operation::AddEdge(name.to_string(), from.to_string(), to.to_string());
            transform_and_print(g, op, t);
        }
        Rule::AddVertexProperty => {
            let mut val_iter = transfos.into_inner();
            let node = val_iter.next().unwrap().as_str();
            let prop = val_iter.next().unwrap().as_str();
            let ptype = val_iter.next().unwrap().as_str();
            let op = Operation::AddVertexProperty(node.to_string(), prop.to_string(), ptype.to_string());
            transform_and_print(g, op, t);
        }
        _ => {}
    }
}

fn main() {
    let _ = IDEMPOTENCE.set(true);
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let parser = PropertyGraphParser;
    let schema_text = read_to_string(args.arg_schema).unwrap();
    let mut schema = parser.convert_text(&schema_text).pop().unwrap();

    let target_text = read_to_string(args.arg_target).unwrap();
    let target = parser.convert_text(&target_text).pop().unwrap();

    let transfos_text = read_to_string(args.arg_transfofile).unwrap();
    let parsed = TransfosParser::parse(Rule::Main, &transfos_text).unwrap().next().unwrap();
    let mut gt = (&schema).into();
    transform(&mut gt, parsed, &target);
    println!("{}", gt);
}
