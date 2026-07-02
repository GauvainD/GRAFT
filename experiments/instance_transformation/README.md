# Instance Transformation

Scripts and results for transforming graph database instances (schema + data)
of the iBench and ICIJ (Paradise Papers) datasets, and for measuring the cost
of applying those transformations in Neo4j.

## Workflow

1. **`data-gen/`** — generate the data.
   - `config.txt` is the iBench configuration used to generate the schema and
     data. The seed is used to ensure identical schema generation at each run.
   - `gendata.py` populates the CSV data files iBench itself fails to write
     (iBench crashes before completing this step), so it re-derives them from
     the generated schema. Edit `INDIR`/`OUTDIR`/`NUM` at the top of the
     script before running.
2. **`load-data/`** — import the generated schema and data into Neo4j.
   - `import_schema.py` parses a `.pgschema` file (using `lark`) and loads
     the corresponding nodes/edges from the CSVs into Neo4j via `bolt://localhost:7687`.
   - `sources.pgschema` / `target.pgschema` describe the source and target
     graph schemas (property graph schema notation) for the iBench dataset.
   - Not needed for ICIJ: a ready-to-load database dump is available on the
     [ICIJ Offshore Leaks website](https://offshoreleaks.icij.org/).
3. **`apply_transfo/`** — apply schema transformations to the loaded instance
   and time each step.
   - `run_transfo.py` holds the shared Cypher helpers (`create_node`,
     `add_prop`, `del_prop`, `create_edge`, `create_edge_from_edge`, ...)
     used to express transformations as sequences of graph operations.
   - `ibench_transfo.py` defines the iBench transformation scenarios
     (`merge`, `split`, `add_prop_transfo`, `rem_prop_transfo`).
   - `icij_transfo.py` defines the ICIJ transformation scenarios (`r9_1`,
     `r1_3_1`, `r10_13_1`, ...).
   - Each script calls one scenario function at the bottom; comment/uncomment
     the relevant call(s) to choose which transformation to run.
4. **`results/`** — captured output (per-step timing and Neo4j summary
   counters) from running the transformations above.
   - `ibench/` — `add_prop.txt`, `merge.txt`, `rem_prop.txt`, `split.txt`:
     one file per iBench transformation.
   - `icij/` — `icij.txt`, `icij_transfos.txt`: ICIJ transformation runs.
   - `scaled/` — `sf1_merge.txt`, `sf2_merge.txt`: the merge transformation
     run against iBench scaled at factors 1 and 2 (scale factor 3 is the
     `merge` result under `ibench/`).

## Prerequisites

- A running Neo4j instance reachable at `bolt://localhost:7687`.
- Python packages: `neo4j`, `pandas`, `lark`.
