//! Smoke tests for the PGSchema parser (`transproof::parsing`).

use transproof::parsing::PropertyGraphParser;

const SIMPLE: &str = "CREATE GRAPH TYPE {
    (personType: Person & Human { name TEXT, age INT }),
    (cityType: City { name TEXT }),
    (:personType)-[livesType: LIVES_IN { since INT }]->(:cityType)
}";

const CONSTRAINTS: &str = "
CREATE GRAPH TYPE socialGraphType {
    ( personType : Person { name STRING , id INT }) ,
    ( customerType : Customer { id INT }) ,
    (: personType )
    -[ friendType : Knows & Likes & Bestie ] - >
    (: personType ) ,
    FOR ( x : personType )
    EXCLUSIVE MANDATORY SINGLETON x . id ,
    FOR ( x : customerType )
    MANDATORY y . id WITHIN ( y : personType ) WHERE y . id = x . id ,
    FOR ( x : personType )
    SINGLETON y WITHIN ( x ) -[ y : friendType & Bestie ] - > ()
}
";

#[test]
fn parses_constraints() {
    let graphs = PropertyGraphParser.convert_text(CONSTRAINTS);
    let g = &graphs[0];
    assert_eq!(g.constraints.len(), 3);
    assert!(g.constraints.iter().all(|c| c.validate(g)));
    let texts: Vec<String> = g.constraints.iter().map(|c| c.text(g, false)).collect();
    assert_eq!(
        texts,
        [
            "FOR (x:personType) EXCLUSIVE MANDATORY SINGLETON x.id",
            "FOR (x:customerType) MANDATORY y.id WITHIN (y:personType) WHERE y.id = x.id",
            "FOR (x:personType) SINGLETON y WITHIN (x)-[y:friendType&Bestie]->()",
        ]
    );
}

#[test]
fn parses_conjunction_of_matches() {
    let input = "CREATE GRAPH TYPE {
        (personType: Person { name STRING, city STRING }),
        (cityType: City { name STRING }),
        (:personType)-[friendType: Knows]->(:personType),
        (:personType)-[livesType: LivesIn]->(:cityType),
        FOR (x: personType), (x)-[f: friendType]->(y: personType) WHERE DIST(x.city, y.city) <= 2
        MANDATORY c WITHIN (x)-[: livesType]->(c: cityType), (y)-[: livesType]->(c)
        WHERE c.name = x.city,
        FOR (x: personType) EXCLUSIVE x.name, x.city
    }";
    let g = &PropertyGraphParser.convert_text(input)[0];
    assert_eq!(g.constraints.len(), 2);
    assert!(g.constraints.iter().all(|c| c.validate(g)));
    let def = &g.constraints[0].def;
    assert_eq!(def.lhs.atoms.len(), 2);
    assert_eq!(def.rhs.atoms.len(), 2);
    assert_eq!(def.lhs.conditions.len(), 1);
    assert_eq!(def.rhs.conditions.len(), 1);
    assert_eq!(g.constraints[1].def.keys.len(), 2);
}

#[test]
fn parses_nodes_edges_labels_and_properties() {
    let graphs = PropertyGraphParser.convert_text(SIMPLE);
    assert_eq!(graphs.len(), 1);
    let g = &graphs[0];

    assert_eq!(g.graph.node_count(), 2);
    assert_eq!(g.graph.edge_count(), 1);

    let person = g
        .graph
        .node_weights()
        .find(|p| p.name == "personType")
        .expect("personType node");
    assert_eq!(person.map.get("name").map(String::as_str), Some("TEXT"));
    assert_eq!(person.map.get("age").map(String::as_str), Some("INT"));

    let edge = g.graph.edge_weights().next().unwrap();
    assert_eq!(edge.name, "livesType");
    assert_eq!(edge.map.get("since").map(String::as_str), Some("INT"));

    // Labels: Person, Human, City on vertices; LIVES_IN on the edge.
    assert_eq!(g.vertex_label.labels().count(), 3);
    assert_eq!(g.edge_label.labels().count(), 1);
}

#[test]
fn parses_multiple_graph_types() {
    let input = format!("{SIMPLE}\n{SIMPLE}");
    assert_eq!(PropertyGraphParser.convert_text(&input).len(), 2);
}

#[test]
fn parses_empty_graph_type() {
    let graphs = PropertyGraphParser.convert_text("CREATE GRAPH TYPE { }");
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0].graph.node_count(), 0);
}

#[test]
fn parses_bundled_pgschema_file() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_inputs/ibench_large_target.pgschema");
    let text = std::fs::read_to_string(path).expect("read test input");
    let graphs = PropertyGraphParser.convert_text(&text);
    assert_eq!(graphs.len(), 1);
    assert!(graphs[0].graph.node_count() > 0);
}

#[test]
#[should_panic]
fn rejects_malformed_input() {
    PropertyGraphParser.convert_text("CREATE GRAPH TYPE { (broken");
}
