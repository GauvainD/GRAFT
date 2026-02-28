# Experiments

The file `make_all_plots.py` produces all plots from the paper in a directory
`plot`. This directory will be created if it does not already exist.


The experiment data is provided in directory `results`. They are in csv format.
Because computation was performed 5 times, each entry is an array of 5 values.
`None` indicates a timeout.

- `index`: Index of each line. Format depends of the experiment.
- `similarity`: Best similarity reached.
- `path`: Number of edit operations in the final transformation between source
and the best schema.
- `time`: Total computation time in seconds.
- `souffle_time`: Cumulative time to evaluate the datalog program.
- `neo4j_time`: Cumulative time to evaluate the compute the meta-graph.
- `sim_time`: Cumulative time to evaluate the compute the similarity.
- `gen_time`: Cumulative time to evaluate the compute the schemas from a grounded meta-transformation.
- `automaton_time`: Cumulative time to build the meta-transformation from
souffle's results and contract cliques.
- `transfo_path`: Final sequence of transformations from source to the best
schema. A special format is used. ";" separates edit operations within a
transformation and ":" separates transformations in the same sequence. This
format being large, it was only computed in the
`cand-schema-comparison-path.csv` file.

The files are as follows:

- `cand-schema-comparison.csv`: Measures when varying the number of
candidate schemas without storing the sequences of transformations.
- `cand-schema-comparison-path.csv`: Measures when varying the number of
candidate schemas. Does not contain the `naive` strategy since results are
identical to `greedy` and it takes much longer.
- `dataset-comparison.csv`: Comparison between different datasets.
- `schema-size-comparison.csv`: Evolution of results when artificially
increasing the size of schemas.
- `similarities.csv`: Similarity between each source and target schema pairs.
The `inserted` column refers to the number of random nodes inserted. When
`inserted`=0, this is the original source schema.
- `similarity-comparison.csv`: Comparison of results when using increasing
number of samples for the Minhash similarity. A value of `None` for the sample
size means the full Jaccard index was used.
- `theta-comparison.csv`: Comparison of results when varying the parameter
`theta`.
