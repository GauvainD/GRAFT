# Results

The file `make_all_plots.py` produces all plots from the paper in the
`plots/` directory, which is created automatically if it does not exist.
Layout constants (figure height, spacing, legend position) are in `params.py`.

## Running

```bash
python make_all_plots.py
```

Dependencies: `pandas`, `seaborn`, `matplotlib`, `numpy`.

## Output figures

| File | Contents |
|---|---|
| `plots/figure6.pdf` | Similarity improvement, runtime, and edit-op count vs. number of candidate schemas, per dataset and strategy |
| `plots/figure7.pdf` | Average transformation size vs. candidate schemas; similarity and runtime vs. schema size (added nodes) |
| `plots/figure8.pdf` | Dataset comparison, theta (θ) sensitivity, and MinHash sample size sensitivity |
| `plots/figure9.pdf` | Stacked-bar breakdown of runtime phases (greedy strategy) |

## CSV data (`csv/`)

Each CSV records measurements over 5 repeated runs, so numeric columns are
arrays of 5 values. `None` indicates a timeout.

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
| `transfo_path` | Final transformation sequence (`";"` separates edit operations within a transformation, `":"` separates transformations in a sequence) |

### Files

| File | Used in | Description |
|---|---|---|
| `cand-schema-comparison.csv` | Figure 6, Figure 7 | Varying number of candidate schemas; `transfo_path` not stored |
| `cand-schema-comparison-path.csv` | Figure 7 | Same experiment with `transfo_path` stored; excludes `naive` (identical to `greedy`, much slower) |
| `dataset-comparison.csv` | Figure 8, Figure 9 | Comparison across all four datasets |
| `schema-size-comparison.csv` | Figure 7 | Results when artificially increasing schema size by inserting random nodes |
| `similarities.csv` | Figures 6–8 (baseline) | Jaccard similarity between each source/target pair; `inserted` = number of random nodes added (0 = original) |
| `similarity-comparison.csv` | Figure 8 | Varying MinHash sample size; `None` = exact Jaccard |
| `theta-comparison.csv` | Figure 8 | Varying the minimum similarity threshold θ |
