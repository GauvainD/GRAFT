# Results

The file `make_all_plots.py` produces all figures used in the paper into the
`plots/` directory, which is created automatically if it does not exist.

## Running

```bash
# from experiments/results/ (CSV/output paths are relative to this directory)
python3 make_all_plots.py                 # build all 8 figures
python3 make_all_plots.py figure7         # build just one
python3 make_all_plots.py --outdir myout  # override output directory
python3 make_all_plots.py --list          # list figure names
```

Dependencies: `pandas`, `polars`, `seaborn`, `matplotlib`, `numpy`.

## Output figures

Each row is one entry of the `BUILDERS` dispatch table in `make_all_plots.py`
(`<name>` is the CLI argument, output file is `plots/<name>.pdf`). The "Paper
figure" column gives the corresponding file in the paper's `figures/`
directory and the label/caption it appears under.

| `<name>` | Output file | Contents | Paper figure |
|---|---|---|---|
| `transformation_details` | `plots/transformation_details.pdf` | Per-edit-op impacted elements and runtime bars | `figures/transformation_details.pdf`, `fig:transfos_elems` (appendix) |
| `figure7` | `plots/figure7.pdf` | Theta (θ) sweep and increasing schema size (similarity & runtime) | `figures/figure7.pdf`, `fig:scalability-experiment` (appendix) |
| `figure8` | `plots/figure8.pdf` | Average transformation size (pruning), per-dataset metrics, MinHash sample size sweep | `figures/figure8.pdf`, `fig:dataset-experiment` |
| `alt_stackbars` | `plots/alt_stackbars.pdf` | GRAFT runtime breakdown, stacked horizontal bars per dataset | `figures/alt_stackbars.pdf`, `fig:timings` (appendix) |
| `dblp_pruning` | `plots/dblp_pruning.pdf` | Single-dataset (`dblp_to_amalgam1`) pruning sweep | `figures/dblp_pruning.pdf`, `fig:dblp-pruning` (appendix) |
| `figure6_beam` | `plots/figure6_beam.pdf` | Pruning blocks (top) stacked over beam-width blocks (bottom) | `figures/figure6_beam.pdf`, `fig:pruning-experiment` |
| `icij_instance` | `plots/icij_instance.pdf` | ICIJ instance transformation, runtime & element-count bars | `figures/icij_instance.pdf`, `fig:icij-instance` (appendix) |
| `instance_experiment` | `plots/instance_experiment.pdf` | Scaled iBench (top) + large-scale iBench (bottom) | `figures/instance_experiment.pdf`, `fig:ibench-result` |

## CSV data (`csv/`)

Each CSV records measurements over 5 repeated runs, so numeric columns are
arrays of 5 values (unless noted otherwise below). `None` indicates a
timeout.

### Common columns

| Column | Description |
|---|---|
| `index` | Row identifier; format depends on the experiment |
| `similarity` | Best similarity reached |
| `path` | Number of edit operations in the final transformation |
| `time` | Total computation time (s) |
| `souffle_time` | Cumulative time to evaluate the Datalog program |
| `neo4j_time` | Cumulative time to compute the meta-graph |
| `sim_time` | Cumulative time to compute similarity |
| `gen_time` | Cumulative time to generate schemas from a grounded meta-transformation |
| `automaton_time` | Cumulative time to build the meta-transformation automaton and contract cliques |
| `num_dup` | Number of duplicate (pruned) schemas encountered |
| `num_tot` | Total number of schemas explored |
| `transfo_path` | Final transformation sequence (`";"` separates edit operations within a transformation, `":"` separates transformations in a sequence) |

Not every file has every column — see the per-file table below.

### Files

| File | Used in (`make_all_plots.py` builder) | Description |
|---|---|---|
| `cand-schema-comparison.csv` | `figure7`, `figure8`, `dblp_pruning`, `figure6_beam` | Varying number of candidate schemas; `transfo_path` not stored |
| `cand-schema-comparison-path.csv` | `figure8`, `dblp_pruning`, `figure6_beam` | Same experiment with `transfo_path` stored|
| `dataset-comparison.csv` | `figure8`, `alt_stackbars` | Comparison across all four datasets; includes `num_dup`/`num_tot` |
| `schema-size-comparison.csv` | `figure7` | Results when artificially increasing schema size by inserting random nodes |
| `similarities.csv` | `figure7`, `figure8`, `dblp_pruning`, `figure6_beam` | Jaccard similarity between each source/target pair; `inserted` = number of random nodes added (0 = original) |
| `similarity-comparison.csv` | `figure8` | Varying MinHash sample size; `None` = exact Jaccard; includes `num_dup`/`num_tot` |
| `theta-comparison.csv` | `figure7`, `figure8` | Varying the minimum similarity threshold θ |
| `nok.csv` | `figure8`, `dblp_pruning`, `figure6_beam` | "No pruning"/unbounded baseline runs; includes `num_dup`/`num_tot`, no `transfo_path` |
| `beam.csv` | `figure6_beam` | Beam-width sweep; `index` format `<dataset>-<strategy>-<beam width>`; includes `num_dup`/`num_tot`, `transfo_path` |
| `transformations_ibench.csv` | `transformation_details` | Per-edit-operation timing/impacted-element breakdown (`transfo, operation, time, num_elems`); one row per operation instance, scalar values (not 5-run arrays) |
| `transformations_icij.csv` | `icij_instance` | ICIJ transformation trace (`transfo, type, elems, time, sim, size`); one row per applied transformation, `type=start` is the baseline |
| `all_transfos_ibench.csv` | `instance_experiment` | Per-transformation-type summary (`transfo, elems, time, sim, size`); scalar values, one row per transformation type |
| `sf_data.csv` | `instance_experiment` | Scale-factor rows (`sf, elems, time`); scalar values, one row per scale factor |
| `ibench_huge.csv` | *(unused — `load_ibench_data` in `make_all_plots.py` is not called by any builder)* | Large-scale iBench sweep, same column shape as `beam.csv`; also read directly by the standalone `plot_huge.py` (see below) |

## Auxiliary scripts

Three scripts at the top level of `experiments/results/` are not part of the
`make_all_plots.py` pipeline and are not guaranteed to run as-is:

- `make_ibench_plots.py` — reads `csv/persondata-k.csv` and
  `csv/ibench_large_no_path.csv`, neither of which currently exists in
  `csv/`; it will fail before producing output.
- `plot_beam.py` — reads `./beam.csv` (should be `csv/beam.csv`) and writes
  its output to `/tmp/beam.pdf`, outside the repo.
- `plot_huge.py` — reads `csv/ibench_huge.csv` (works) and writes
  `ibench_huge.pdf` to the current directory rather than `plots/`.

`params.py` defines layout constants but is not currently imported by
`make_all_plots.py`, which redefines the same constants inline.
