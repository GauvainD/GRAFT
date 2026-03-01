//! The pipeline runs in the following phases:
//!
//! 1. **Input** — PGSchema files (or Neo4j) are parsed into [`property_graph::PropertyGraph`] values.
//! 2. **Souffle / Datalog** — user-supplied `.dl` programs are compiled by the build script and
//!    driven at runtime via [`transformation::souffle`] FFI to emit an operation automaton.
//! 3. **Transformation automaton** — [`transformation_automaton`] reads the Souffle `Next`
//!    relations and builds a directed graph of sequencing constraints, contracting commutative
//!    cliques into subset-generator nodes.
//! 4. **Graph iteration** — [`compute`] performs a DFS through the automaton, applying each
//!    reachable operation sequence to the source schema in parallel (via `rayon`).
//! 5. **Similarity filtering** — each candidate is scored against the target schema using
//!    [`similarity`] (Jaccard index or MinHash) and the best-N results are kept.
//! 6. **Output** — results are written to stdout, a text file, or a Neo4j database.

/// Transformation application, and result aggregation.
pub mod compute;
/// Global compile-time and runtime tuning constants.
pub mod constants;
/// Error types used across the library.
pub mod errors;
/// Low-level graph mutation operations that implement the `Operation` variants.
pub mod graph_transformation;
/// Async Neo4j integration for reading source schemas and writing results.
pub mod neo4j;
/// PGSchema text parser (pest grammar).
pub mod parsing;
/// Core property graph schema data structure (petgraph wrapper with labels and properties).
pub mod property_graph;
/// Jaccard index and MinHash similarity computation.
pub mod similarity;
/// `Operation` / `OperationName` enums and Souffle FFI bridge.
pub mod transformation;
/// Automaton construction from Souffle `Next` relations, clique contraction, and graph iteration.
pub mod transformation_automaton;
