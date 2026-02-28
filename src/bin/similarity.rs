//! Compute similarity between two schemas.
use std::fs::File;
use std::io::BufReader;
use std::io::Read;

use docopt::Docopt;
use probminhash::jaccard::compute_probminhash_jaccard;
use serde::Deserialize;
use transproof::similarity::jaccard_index;
use transproof::similarity::property_graph_minhash;
use transproof::{parsing::PropertyGraphParser, property_graph::PropertyGraph};

const USAGE: &str = "
Similarity is a tool to compute the similarity between two graphs.

Usage:
    similarity [options] <graph1> <graph2>
    similarity (-h | --help)

Options:
    -h, --help             Show this message.
    --minshash             Use minhash similarity instead of default jaccard index.
";

#[derive(Debug, Deserialize)]
struct Args {
    flag_minshash: bool,
    arg_graph1: String,
    arg_graph2: String,
}

/// Reads the schema from a file.
fn load_graph(path: &str) -> PropertyGraph {
    let mut buf = BufReader::new(File::open(path).unwrap());
    let mut text = String::new();
    buf.read_to_string(&mut text).unwrap();
    let parser = PropertyGraphParser;
    let mut v = parser.convert_text(&text);
    if v.len() != 1 {
        panic!("Only one target schema is supported. Found {}.", v.len());
    }
    let target = v.drain(0..1).next().unwrap();
    target
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    let graph1 = load_graph(&args.arg_graph1);
    let graph2 = load_graph(&args.arg_graph2);
    if args.flag_minshash {
        let sig1 = property_graph_minhash(&graph1, 100);
        let sig2 = property_graph_minhash(&graph2, 100);
        println!("Minhash similarity: {}", compute_probminhash_jaccard(&sig1, &sig2));
    } else {
        println!("Jaccard index: {}", jaccard_index(&graph1, &graph2));
    }
}
