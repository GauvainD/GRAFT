use std::{collections::{HashMap, HashSet}, sync::LazyLock};
use std::fs::read_to_string;

use petgraph::graph::NodeIndex;
use regex::Regex;
use serde::Deserialize;
use quick_xml::de::from_str;
use transproof::property_graph::{Properties, PropertyGraph};


#[derive(Debug, Deserialize)]
struct Scenario {
    #[serde(rename = "Schemas")]
    schemas: Schemas,
    #[serde(rename="Correspondences")]
    correspondences: Correspondences,
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
struct Correspondences {
    #[serde(rename="Correspondence")]
    correspondences: Vec<Correspondence>,
}

#[derive(Debug, Deserialize)]
struct Correspondence {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "From")]
    from: CorrespondenceItem,
    #[serde(rename = "To")]
    to: CorrespondenceItem,
}

#[derive(Debug, Deserialize)]
struct CorrespondenceItem {
    #[serde(rename = "@tableref")]
    table: String,
    #[serde(rename = "Attr", default)]
    attr: String,
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

/// Finds the first node whose name matches `name` after stripping the copy suffix.
fn find_node(pg: &PropertyGraph, name: &str) -> Option<NodeIndex> {
    let stripped = strip_copy_suffix(name);
    pg.graph.node_indices()
        .find(|&idx| pg.graph.node_weight(idx).map(|p| p.name == stripped).unwrap_or(false))
}

/// Renames node properties in-place using a `old_name -> new_name` map.
fn rename_node_props(pg: &mut PropertyGraph, idx: NodeIndex, renames: &HashMap<String, String>) {
    if let Some(node) = pg.graph.node_weight_mut(idx) {
        let map = std::mem::take(&mut node.map);
        node.map = map.into_iter()
            .map(|(k, v)| (renames.get(&k).cloned().unwrap_or(k), v))
            .collect();
    }
}

/// Copy case (1 source → 1 target): rename the target node and its properties to source names.
fn apply_copy(
    pg: &mut PropertyGraph,
    source_table: &str,
    target_table: &str,
    prop_renames: &HashMap<String, String>, // target_attr -> source_attr
) {
    if let Some(idx) = find_node(pg, target_table) {
        if let Some(node) = pg.graph.node_weight_mut(idx) {
            node.name = source_table.to_string();
        }
        rename_node_props(pg, idx, prop_renames);
    }
}

/// Split case (1 source → N targets): drop FK columns from each target node, rename props to source names.
fn apply_split(
    pg: &mut PropertyGraph,
    target_tables: &[String],
    prop_renames: &HashMap<String, HashMap<String, String>>, // target_table -> {target_attr -> source_attr}
    mapped_target_attrs: &HashMap<String, HashSet<String>>,  // target_table -> set of mapped attrs
    target_schema: &Schema,
) {
    // Pass 1: find the single edge name — the globally smallest unmapped attr across all target nodes.
    let mut edge_name: Option<String> = None;
    for target_table in target_tables {
        let empty = HashSet::new();
        let mapped = mapped_target_attrs.get(target_table).unwrap_or(&empty);
        if let Some(rel) = target_schema.relations.iter().find(|r| &r.name == target_table) {
            for attr in &rel.attrs {
                if !mapped.contains(&attr.name) {
                    let is_smaller = edge_name.as_ref().map(|m| &attr.name < m).unwrap_or(true);
                    if is_smaller {
                        edge_name = Some(attr.name.clone());
                    }
                }
            }
        }
    }
    let edge_name = edge_name.unwrap_or_default();

    // Pass 2: remove all unmapped attrs from each node, rename remaining props, collect indices.
    let mut node_indices = vec![];
    for target_table in target_tables {
        let empty = HashSet::new();
        let mapped = mapped_target_attrs.get(target_table).unwrap_or(&empty);

        if let Some(idx) = find_node(pg, target_table) {
            let mapped_clone = mapped.clone();
            if let Some(node) = pg.graph.node_weight_mut(idx) {
                node.map.retain(|k, _| mapped_clone.contains(k));
            }
            if let Some(renames) = prop_renames.get(target_table) {
                rename_node_props(pg, idx, renames);
            }
            node_indices.push(idx);
        }
    }
}

/// Merge case (N sources → 1 target): rename target props to source names, then retain only
/// attrs that exist in the source tables (by source attr name).
fn apply_merge(
    pg: &mut PropertyGraph,
    target_table: &str,
    prop_renames: &HashMap<String, HashMap<String, String>>, // target_table -> {target_attr -> source_attr}
    source_attrs: &HashSet<String>,                          // all attr names across source tables
) {
    if let Some(target_idx) = find_node(pg, target_table) {
        let empty_map = HashMap::new();
        let renames = prop_renames.get(target_table).unwrap_or(&empty_map);
        rename_node_props(pg, target_idx, renames);
        if let Some(node) = pg.graph.node_weight_mut(target_idx) {
            node.map.retain(|k, _| source_attrs.contains(k));
        }
    }
}

/// Converts an iBench scenario to a PropertyGraph by interpreting each mapping and applying
/// the appropriate copy/split/merge transformation to the target schema, normalising it to
/// use source attribute names throughout.
fn scenario_to_property_graph(scenario: &Scenario) -> PropertyGraph {
    let mut pg = schema_to_property_graph(&scenario.schemas.target);

    let corr_map: HashMap<&str, &Correspondence> = scenario.correspondences.correspondences
        .iter().map(|c| (c.id.as_str(), c)).collect();

    for mapping in &scenario.mappings.mappings {
        let source_tables: Vec<String> = mapping.foreach.atoms.iter()
            .map(|a| a.tableref.clone()).collect();
        let target_tables: Vec<String> = mapping.exists.atoms.iter()
            .map(|a| a.tableref.clone()).collect();

        let mut prop_renames: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut mapped_target_attrs: HashMap<String, HashSet<String>> = HashMap::new();

        for cref in &mapping.uses.correspondences {
            if let Some(corr) = corr_map.get(cref.cref.as_str()) {
                prop_renames.entry(corr.to.table.clone()).or_default()
                    .insert(corr.to.attr.clone(), corr.from.attr.clone());
                mapped_target_attrs.entry(corr.to.table.clone()).or_default()
                    .insert(corr.to.attr.clone());
            }
        }

        match (source_tables.len(), target_tables.len()) {
            (1, 1) => {
                let empty_map = HashMap::new();
                let renames = prop_renames.get(&target_tables[0]).unwrap_or(&empty_map).clone();
                apply_copy(&mut pg, &source_tables[0], &target_tables[0], &renames);
            }
            (1, _) => {
                apply_split(&mut pg, &target_tables, &prop_renames, &mapped_target_attrs, &scenario.schemas.target);
            }
            (_, 1) => {
                let source_attrs: HashSet<String> = source_tables.iter()
                    .flat_map(|st| {
                        scenario.schemas.source.relations.iter()
                            .find(|r| &r.name == st)
                            .into_iter()
                            .flat_map(|rel| rel.attrs.iter().map(|a| a.name.clone()))
                    })
                    .collect();
                apply_merge(&mut pg, &target_tables[0], &prop_renames, &source_attrs);
            }
            _ => {}
        }
    }

    pg
}

/// For every merge mapping (N sources → 1 target), drops from each source node any attribute
/// that has no correspondence to the target table (i.e. join attributes).
fn remove_merge_join_attrs(pg: &mut PropertyGraph, scenario: &Scenario) {
    let corr_map: HashMap<&str, &Correspondence> = scenario.correspondences.correspondences
        .iter().map(|c| (c.id.as_str(), c)).collect();

    for mapping in &scenario.mappings.mappings {
        if mapping.foreach.atoms.len() <= 1 || mapping.exists.atoms.len() != 1 {
            continue;
        }
        let mapped: HashSet<(String, String)> = mapping.uses.correspondences.iter()
            .filter_map(|cr| corr_map.get(cr.cref.as_str()))
            .map(|c| (c.from.table.clone(), c.from.attr.clone()))
            .collect();

        for atom in &mapping.foreach.atoms {
            let st = atom.tableref.clone();
            if let Some(idx) = find_node(pg, &st) {
                if let Some(node) = pg.graph.node_weight_mut(idx) {
                    node.map.retain(|k, _| mapped.contains(&(st.clone(), k.clone())));
                }
            }
        }
    }
}

fn main() {
    let args = std::env::args();
    let filename = args.skip(1).take(1).next().unwrap();
    let xmlinput = read_to_string(filename).unwrap();
    let parsed: Scenario = from_str(&xmlinput).unwrap();
    let mut source = schema_to_property_graph(&parsed.schemas.source);
    remove_merge_join_attrs(&mut source, &parsed);
    let target = scenario_to_property_graph(&parsed);
    println!("Source:\n{source}");
    println!("Target:\n{target}");
}
