use std::{collections::HashMap, sync::LazyLock};
use std::fs::read_to_string;

use regex::Regex;
use serde::Deserialize;
use quick_xml::de::from_str;
use transproof::property_graph::{Properties, PropertyGraph};


#[derive(Debug, Deserialize)]
struct Scenario {
    #[serde(rename = "Schemas")]
    schemas: Schemas,
    #[serde(rename = "Mappings")]
    mappings: Mappings,
}

#[derive(Debug, Deserialize)]
struct Schemas {
    #[serde(rename = "SourceSchema")]
    source: Schema,
    #[serde(rename = "TargetSchema")]
    target: Schema,
}

#[derive(Debug, Deserialize)]
struct Schema {
    #[serde(rename = "Relation", default)]
    relations: Vec<Relation>,
    #[serde(rename = "ForeignKey", default)]
    foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Deserialize)]
struct Relation {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "Attr", default)]
    attrs: Vec<RelAttr>,
}

#[derive(Debug, Deserialize)]
struct RelAttr {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "DataType")]
    data_type: String,
}

#[derive(Debug, Deserialize)]
struct ForeignKey {
    #[serde(rename = "From")]
    from: FKRef,
    #[serde(rename = "To")]
    to: FKRef,
}

#[derive(Debug, Deserialize)]
struct FKRef {
    #[serde(rename = "@tableref")]
    tableref: String,
    #[serde(rename = "Attr", default)]
    attrs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Mappings {
    #[serde(rename = "Mapping", default)]
    mappings: Vec<Mapping>,
}

#[derive(Debug, Deserialize)]
struct Mapping {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "Uses")]
    uses: Uses,
    #[serde(rename = "Foreach")]
    foreach: AtomList,
    #[serde(rename = "Exists")]
    exists: AtomList,
}

#[derive(Debug, Deserialize)]
struct Uses {
    #[serde(rename = "Correspondence", default)]
    correspondences: Vec<CorrespondenceRef>,
}

#[derive(Debug, Deserialize)]
struct CorrespondenceRef {
    #[serde(rename = "@ref")]
    cref: String,
}

#[derive(Debug, Deserialize)]
struct AtomList {
    #[serde(rename = "Atom", default)]
    atoms: Vec<Atom>,
}

#[derive(Debug, Deserialize)]
struct Atom {
    #[serde(rename = "@tableref")]
    tableref: String,
    #[serde(rename = "Var", default)]
    vars: Vec<String>,
    #[serde(rename = "SKFunction", default)]
    sk_functions: Vec<SKFunction>,
}

#[derive(Debug, Deserialize)]
struct SKFunction {
    #[serde(rename = "@skname")]
    skname: String,
    #[serde(rename = "Var", default)]
    vars: Vec<String>,
}
/// Strips a trailing `copy{n}_{n}` suffix (e.g. `copy0_0`, `copy1_1`) from a relation name.
fn strip_copy_suffix_old(name: &str) -> &str {
    // Walk backwards: digits, '_', same digits, "copy"
    let bytes = name.as_bytes();
    let mut i = bytes.len();
    // trailing digits
    while i > 0 && bytes[i - 1].is_ascii_digit() { i -= 1; }
    let suffix_digits = &name[i..];
    if suffix_digits.is_empty() { return name; }
    // underscore
    if i == 0 || bytes[i - 1] != b'_' { return name; }
    i -= 1;
    // same digits before underscore
    let mut j = i;
    while j > 0 && bytes[j - 1].is_ascii_digit() { j -= 1; }
    if &name[j..i] != suffix_digits { return name; }
    i = j;
    // "copy"
    if !name[..i].ends_with("copy") { return name; }
    &name[..i - 4]
}

/// Strips a trailing `copy{n}_{n}` suffix (e.g. `copy0_0`, `copy1_1`) from a relation name.
fn strip_copy_suffix(name: &str) -> String {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"copy\d+_\d+$").unwrap());
    RE.replace(name, "").to_string()
}

fn schema_to_property_graph(schema: &Schema) -> PropertyGraph {
    let mut pg = PropertyGraph::default();
    let mut node_map = HashMap::new();

    for relation in &schema.relations {
        let props = Properties {
            name: strip_copy_suffix(&relation.name).to_string(),
            map: relation.attrs.iter().map(|a| (a.name.clone(), a.data_type.clone())).collect(),
        };
        let idx = pg.graph.add_node(props);
        node_map.insert(relation.name.clone(), idx);
    }

    for fk in &schema.foreign_keys {
        if let (Some(&from), Some(&to)) = (
            node_map.get(&fk.from.tableref),
            node_map.get(&fk.to.tableref),
        ) {
            let edge_name = fk.from.attrs.first().cloned().unwrap_or_default();
            pg.graph.add_edge(from, to, Properties { name: edge_name, map: HashMap::new() });
        }
    }

    pg
}

fn main() {
    let xmlinput = read_to_string("../../test_inputs/metadata.xml").unwrap();
    let parsed: Scenario = from_str(&xmlinput).unwrap();
    let source = schema_to_property_graph(&parsed.schemas.source);
    let target = schema_to_property_graph(&parsed.schemas.target);
    println!("{source}");
    println!("{target}");
}
