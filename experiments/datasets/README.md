# Datasets

This directory contains the datasets used for the experiments. Each
sub-directory corresponds to one dataset and contains:

- `sources.pgschema` — the source schema
- `target.pgschema` — the target schema
- `transfos.dl` — the meta-transformations as a Datalog program

Most datasets also contain an `augmented/` sub-directory. In here are the source
and target schemas that were augmented by adding `x` random nodes for files
`sources-x.pgschema` and `target-x.pgschema`. These were produced by the
`add_nodes_random` binary, whose source is `src/bin/add_nodes_random.rs`
(built as part of the main `cargo build`).

The `ibench_huge` dataset also contains a `config.txt` configuration file
that was used to generate it using the iBench tool.

## Datasets

| Directory | Description | Augmented |
|---|---|---|
| `dblp_to_amalgam1` | DBLP → Amalgam1 schema migration | yes |
| `amalgam1_to_amalgam3` | Amalgam1 → Amalgam3 schema migration | yes |
| `persondata` | Person data schema migration | yes |
| `flighthotel` | Flight/hotel schema migration | yes |
| `icij` | ICIJ schema migration | no |
| `ibench_huge` | large scale synthetic schema | no |

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

The actual parameter grid is the `run_benchmark(...)` call in the `__main__`
block at the bottom of the script, and is hand-edited for each experiment —
what's currently committed sweeps the `flighthotel` and
`amalgam1_to_amalgam3` datasets over the four selection strategies (`greedy`,
`random`, `weighted_distance`, `naive`) with pruning disabled
(`pruning=0`, i.e. keep every candidate); this is the "no pruning" baseline
sweep behind `nok.csv` in [`experiments/results/`](../results/README.md).
Results are written to `runs/<DEFAULTS["filename"]>` (currently
`runs/nok-two.csv`), cached in `runs/<DEFAULTS["shelve_name"]>` (currently
`runs/nok-rest.shelf`). Edit the `run_benchmark(...)` call to reproduce a
different sweep, e.g. across all four augmented datasets or over MinHash
sample sizes.

### Key configuration constants (top of `benchmark.py`)

| Constant | Current value | Description |
|---|---|---|
| `GRAFT_PATH` | `../../` | Path to the repository root |
| `NEO4J_URI` | `neo4j://localhost` | Neo4j connection URI |
| `TIMEOUT` | `6*3600` (6 hours) | Per-run timeout in seconds |
| `NUM_RUNS` | `5` | Number of repetitions per configuration |
| `OVERWRITE` | `False` | Re-run and overwrite cached results |
| `APPEND` | `False` | Append new runs to existing cached results |
| `PRUNE_TIMEOUTS` | `False` | Drop timed-out entries from the cache |
| `SAVE_TRANSFO_PATH` | `True` | Include transformation paths in the CSV |

These are edited alongside the `run_benchmark(...)` call per experiment, so
treat the values above as "what's currently committed", not fixed defaults.
