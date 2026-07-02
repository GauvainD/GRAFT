# Experiments

This directory is split in three parts:

- `datasets/` — the property graph schema pairs used as input, the Datalog
  meta-transformation programs, and the `benchmark.py` script that runs
  `graft` over parameter combinations and collects results into CSV
  files. See [`datasets/README.md`](datasets/README.md) for details.

- `results/` — the CSV files produced by the benchmark, and the
  `make_all_plots.py` script that regenerates all paper figures from them.
  See [`results/README.md`](results/README.md) for details.

- `instance_transformation/` — scripts to generate and load iBench/ICIJ graph
  *instances* (schema + data) into Neo4j and time the cost of applying schema
  transformations to them, as opposed to `datasets/`+`results/` which only
  work at the schema level. See
  [`instance_transformation/README.md`](instance_transformation/README.md)
  for details.

## Reproducing the experiments

### Run the benchmark

Requires a running Neo4j instance at `neo4j://localhost`.

```bash
cd datasets
pip install neo4j
python benchmark.py
```

Results are written to `datasets/runs/<filename>`, where `<filename>` is set
by `DEFAULTS["filename"]` at the top of `benchmark.py` (see
[`datasets/README.md`](datasets/README.md#running-the-benchmark) — the
committed parameter grid and output filename are edited per experiment).

### Produce the plots

```bash
cd results
python make_all_plots.py
```

Figures are written to `results/plots/`.
