# Datasets

This directory contains the datasets used for the experiments. Each
sub-directory corresponds to one dataset and contains:

- `sources.pgschema` — the source schema
- `target.pgschema` — the target schema
- `transfos.dl` — the meta-transformations as a Datalog program

Most datasets also contain an `augmented/` sub-directory. In here are the source
and target schemas that were augmented by adding `x` random nodes for files
`sources-x.pgschema` and `target-x.pgschema`. These were produced by the
`add_nodes_random` binary, which is included in the `bin/` directory.

## Datasets

| Directory | Description | Augmented |
|---|---|---|
| `dblp_to_amalgam1` | DBLP → Amalgam1 schema migration | yes |
| `amalgam1_to_amalgam3` | Amalgam1 → Amalgam3 schema migration | yes |
| `persondata` | Person data schema migration | yes |
| `flighthotel` | Flight/hotel schema migration | yes |
| `icij` | ICIJ schema migration | no |

## Running the benchmark

The `benchmark.py` script runs `graft` over a set of parameter
combinations, collects timing and similarity results, caches them in a
[shelve](https://docs.python.org/3/library/shelve.html) file, and writes a
summary CSV.

### Prerequisites

- A running Neo4j instance at `neo4j://localhost`
- Python dependencies: `neo4j` (`pip install neo4j`)

### Running

```bash
python benchmark.py
```

By default the script iterates over all four augmented datasets, four
selection strategies (`greedy`, `random`, `weighted_distance`, `naive`), and
five MinHash sample sizes (10, 50, 100, 200, and exact Jaccard), running each
combination 5 times. Results are written to `runs/results.csv`.

### Key configuration constants (top of `benchmark.py`)

| Constant | Default | Description |
|---|---|---|
| `GRAFT_PATH` | `../../` | Path to the repository root |
| `NEO4J_URI` | `neo4j://localhost` | Neo4j connection URI |
| `TIMEOUT` | `600` | Per-run timeout in seconds |
| `NUM_RUNS` | `5` | Number of repetitions per configuration |
| `OVERWRITE` | `False` | Re-run and overwrite cached results |
| `APPEND` | `False` | Append new runs to existing cached results |
| `PRUNE_TIMEOUTS` | `False` | Drop timed-out entries from the cache |
| `SAVE_TRANSFO_PATH` | `False` | Include transformation paths in the CSV |
