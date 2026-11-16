//! Module dedicated to parsing the pgschema format for Property Graph Schemas
use std::collections::HashMap;

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use petgraph::graph::{EdgeIndex, NodeIndex};

use crate::{
    constraints::{
        Atom, Condition, Constraint, ConstraintBuilder, ConstraintError, Operand, Term, VarId,
    },
    property_graph::{Properties, PropertyGraph},
};

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
            let text = v.as_str();
            self.build_constraint(v, &names, &enames)
                .unwrap_or_else(|e| panic!("Invalid constraint `{}`: {}", text, e))
        }));
        graph
    }

    /// Parses a single constraint (as written by [`Constraint::text`]) and binds it to the
    /// types of an existing schema. Used to read back the constraints stored in Neo4j.
    pub fn parse_constraint(
        &self,
        text: &str,
        graph: &PropertyGraph,
    ) -> Result<Constraint, ConstraintError> {
        let constraint = PropertyGraphParser::parse(Rule::singleConstraint, text)
            .map_err(|e| ConstraintError::Syntax(e.to_string()))?
            .next()
            .unwrap()
            .into_inner()
            .next()
            .unwrap();
        let names = graph
            .graph
            .node_indices()
            .map(|n| (graph.graph[n].name.clone(), n))
            .collect();
        let enames = graph
            .graph
            .edge_indices()
            .map(|e| (graph.graph[e].name.clone(), e))
            .collect();
        self.build_constraint(constraint, &names, &enames)
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

    /// Builds a constraint and binds it to the node and edge types of the schema being parsed.
    fn build_constraint(
        &self,
        v: Pair<'_, Rule>,
        names: &HashMap<String, NodeIndex>,
        enames: &HashMap<String, EdgeIndex>,
    ) -> Result<Constraint, ConstraintError> {
        let mut b = ConstraintBuilder::new();
        let (mut exclusive, mut mandatory, mut singleton) = (false, false, false);
        let mut keys = None;
        // Matches and conditions of each side (FOR then WITHIN)
        let mut matches = [None, None];
        let mut conditions = [None, None];
        for part in v.into_inner() {
            let (side, within) = match part.as_rule() {
                Rule::constraintSide => (part, 0),
                Rule::constraintWithin => (part.into_inner().next().unwrap(), 1),
                Rule::constraintWhere => {
                    conditions[1] = part.into_inner().next();
                    continue;
                }
                Rule::keyList => {
                    keys = Some(part);
                    continue;
                }
                Rule::constraintType => {
                    match part.as_str() {
                        "EXCLUSIVE" => exclusive = true,
                        "MANDATORY" => mandatory = true,
                        "SINGLETON" => singleton = true,
                        _ => (exclusive, mandatory, singleton) = (true, true, true),
                    }
                    continue;
                }
                _ => continue,
            };
            for part in side.into_inner() {
                match part.as_rule() {
                    Rule::matches => matches[within] = Some(part),
                    Rule::constraintWhere => conditions[within] = part.into_inner().next(),
                    _ => (),
                }
            }
        }
        b.set_qualifiers(exclusive, mandatory, singleton);
        // Matches first: keys and conditions can refer to variables bound later in the text.
        for (within, matches) in matches.into_iter().enumerate() {
            for m in matches.into_iter().flat_map(|m| m.into_inner()) {
                let m = m.into_inner().next().unwrap();
                let atom = match m.as_rule() {
                    Rule::edgeMatch => {
                        let mut from = None;
                        let mut var = None;
                        let mut to = None;
                        for part in m.into_inner() {
                            match part.as_rule() {
                                Rule::nodeMatch if from.is_none() => {
                                    from = Some(self.match_var(part.into_inner().next(), false, &mut b)?)
                                }
                                Rule::nodeMatch => {
                                    to = Some(self.match_var(part.into_inner().next(), false, &mut b)?)
                                }
                                _ => var = Some(self.match_var(Some(part), true, &mut b)?),
                            }
                        }
                        let var = match var {
                            Some(var) => var,
                            None => self.match_var(None, true, &mut b)?,
                        };
                        Atom::Edge {
                            var,
                            from: from.unwrap(),
                            to: to.unwrap(),
                        }
                    }
                    _ => Atom::Node(self.match_var(m.into_inner().next(), false, &mut b)?),
                };
                b.add_atom(within == 1, atom);
            }
        }
        for (within, conditions) in conditions.into_iter().enumerate() {
            for c in conditions.into_iter().flat_map(|c| c.into_inner()) {
                let mut parts = c.into_inner();
                let left = self.term(parts.next().unwrap(), &b)?;
                let op = parts.next().unwrap().as_str().to_string();
                let right = self.term(parts.next().unwrap(), &b)?;
                b.add_condition(within == 1, Condition { left, op, right });
            }
        }
        for key in keys.into_iter().flat_map(|k| k.into_inner()) {
            let key = self.operand(key, &b)?;
            b.add_key(key);
        }
        b.finish().bind(
            |name| names.get(name).copied(),
            |name| enames.get(name).copied(),
        )
    }

    /// Declares the variable of a node or edge match from its `elemSpec` (if any).
    fn match_var(
        &self,
        spec: Option<Pair<'_, Rule>>,
        edge: bool,
        b: &mut ConstraintBuilder,
    ) -> Result<VarId, ConstraintError> {
        let mut name = None;
        let mut ty = None;
        let mut labels = Vec::new();
        for part in spec.into_iter().flat_map(|s| s.into_inner()) {
            match part.as_rule() {
                Rule::constraintVar => name = Some(part.as_str()),
                Rule::elemType => {
                    for inner in part.into_inner() {
                        match inner.as_rule() {
                            Rule::typeName => ty = Some(inner.as_str()),
                            Rule::labelSpecSet => self.extract_labels(inner, &mut labels),
                            _ => (),
                        }
                    }
                }
                _ => (),
            }
        }
        b.var(name, edge, ty, labels)
    }

    /// Converts an `operand`, `constraintElem`, `constraintProp`, `constraintVar` or
    /// `constraintConstant`.
    fn operand(&self, v: Pair<'_, Rule>, b: &ConstraintBuilder) -> Result<Operand, ConstraintError> {
        match v.as_rule() {
            Rule::constraintVar => Ok(Operand::Var(b.lookup(v.as_str())?)),
            Rule::constraintProp => {
                let mut parts = v.into_inner();
                let var = b.lookup(parts.next().unwrap().as_str())?;
                Ok(Operand::Prop(var, parts.next().unwrap().as_str().to_string()))
            }
            Rule::constraintConstant => Ok(Operand::Const(v.as_str().to_string())),
            _ => self.operand(v.into_inner().next().unwrap(), b),
        }
    }

    /// Converts a `term`.
    fn term(&self, v: Pair<'_, Rule>, b: &ConstraintBuilder) -> Result<Term, ConstraintError> {
        let inner = v.into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::dist => {
                let mut parts = inner.into_inner();
                let first = self.operand(parts.next().unwrap(), b)?;
                let second = self.operand(parts.next().unwrap(), b)?;
                Ok(Term::Dist(first, second))
            }
            _ => Ok(Term::Simple(self.operand(inner, b)?)),
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

    const TYPES: &str = "(a: A {k INT}), (b: B {k INT}), (:a)-[e: E & F {w INT}]->(:b)";

    fn parse(constraints: &str) -> PropertyGraph {
        let text = format!("CREATE GRAPH TYPE T {{ {}, {} }}", TYPES, constraints);
        PropertyGraphParser.convert_text(&text).remove(0)
    }

    fn canonical(constraint: &str) -> String {
        let g = parse(constraint);
        assert_eq!(g.constraints.len(), 1);
        assert!(g.constraints[0].validate(&g));
        g.constraints[0].text(&g, true)
    }

    #[test]
    fn whitespace_and_variable_names_do_not_matter() {
        let compact = canonical("FOR (x:a) MANDATORY x.k WHERE x.k = 1");
        let messy = canonical("FOR  ( node : a )\n  MANDATORY\n node . k\n WHERE   node.k  =\n 1");
        assert_eq!(compact, messy);
        assert_eq!(compact, "FOR (v0:a) MANDATORY v0.k WHERE v0.k = 1");
    }

    #[test]
    fn conjunction_of_matches() {
        let text = canonical(
            "FOR (x: a), (x)-[y: e & F]->(z) WHERE y.w > 2 AND x.k <> z.k
             SINGLETON t, t.k WITHIN (x)-[]->(t: b), (: a)-[: e]->(t)",
        );
        assert_eq!(
            text,
            "FOR (v0:a), (v0)-[v1:e&F]->(v2) WHERE v1.w > 2 AND v0.k <> v2.k SINGLETON v3, v3.k \
             WITHIN (v0)-[]->(v3:b), (:a)-[:e]->(v3)"
        );
        // The canonical text is a fixed point
        assert_eq!(canonical(&text), text);
    }

    #[test]
    fn ggd_style_constraint() {
        let g = parse(
            "FOR (x: a), (y: b) WHERE DIST(x.k, y.k) <= 2 MANDATORY f WITHIN (x)-[f: e]->(y) WHERE f.w = x.k",
        );
        let c = &g.constraints[0];
        assert!(c.def.mandatory && !c.def.exclusive && !c.def.singleton);
        let facts = c.facts(&g);
        assert_eq!(facts.nodes.len(), 4);
        assert_eq!(facts.edges, vec![("WITHIN", "v2".to_string(), "v0".to_string(), "v1".to_string())]);
        assert_eq!(facts.conditions[0], ("FOR", 0, "DIST<=".to_string()));
        assert_eq!(facts.terms.iter().filter(|t| t.0 == "FOR").count(), 3);
        assert_eq!(facts.keys, vec![("v2".to_string(), String::new())]);
        assert!(facts.types.contains(&("v2".to_string(), "e".to_string())));
    }

    #[test]
    fn several_constraints_and_identifier() {
        let g = parse("FOR (x: a) IDENTIFIER x.k, FOR (y: b) EXCLUSIVE y.k,");
        assert_eq!(g.constraints.len(), 2);
        let d = &g.constraints[0].def;
        assert!(d.exclusive && d.mandatory && d.singleton);
        assert_eq!(d.node_slots[0].props, vec!["k".to_string()]);
    }

    #[test]
    fn printed_schema_can_be_parsed_back() {
        let g = parse("FOR (x: a) SINGLETON y WITHIN (x)-[y: e & F]->()");
        let again = PropertyGraphParser.convert_text(&g.to_string()).remove(0);
        assert_eq!(again.constraints[0].text(&again, true), g.constraints[0].text(&g, true));
    }

    #[test]
    fn single_constraint_is_bound_to_existing_schema() {
        let g = parse(
            "FOR (x: a), (y: b) WHERE DIST(x.k, y.k) <= 2 AND x.k <> \"a, b\" MANDATORY f \
             WITHIN (x)-[f: e & F]->(y) WHERE f.w = x.k",
        );
        let mut again = parse("FOR (x: a) IDENTIFIER x.k");
        again.constraints.clear();
        for text in [g.constraints[0].text(&g, false), g.constraints[0].text(&g, true)] {
            let c = PropertyGraphParser.parse_constraint(&text, &again).unwrap();
            assert!(c.validate(&again));
            assert_eq!(c.text(&again, true), g.constraints[0].text(&g, true));
        }
        assert!(matches!(
            PropertyGraphParser.parse_constraint("FOR (x: a) MANDATORY x.k, (b: B)", &again),
            Err(ConstraintError::Syntax(_))
        ));
        assert_eq!(
            PropertyGraphParser.parse_constraint("FOR (x: missing) MANDATORY x.k", &again).err(),
            Some(ConstraintError::UnknownNodeType("missing".to_string()))
        );
    }

    #[test]
    #[should_panic(expected = "Unknown node type")]
    fn unknown_type_is_rejected() {
        parse("FOR (x: missing) MANDATORY x.k");
    }

    #[test]
    #[should_panic(expected = "not bound")]
    fn unbound_variable_is_rejected() {
        parse("FOR (x: a) MANDATORY y.k");
    }

    #[test]
    #[should_panic(expected = "two different types")]
    fn conflicting_types_are_rejected() {
        parse("FOR (x: a), (x: b) MANDATORY x.k");
    }

    #[test]
    #[should_panic]
    fn keyword_is_not_a_variable() {
        parse("FOR (FOR: a) MANDATORY FOR.k");
    }

    #[test]
    #[should_panic]
    fn types_must_precede_constraints() {
        PropertyGraphParser.convert_text(
            "CREATE GRAPH TYPE T { (a: A {k INT}), FOR (x: a) MANDATORY x.k, (b: B) }",
        );
    }
}
