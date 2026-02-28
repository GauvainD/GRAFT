# Experiments

This directory contains the datasets used for the experiments. Each
sub-directory contains three files:

- sources.pgschema: the source schema
- target.pgschema: the target schema
- transfos.dl: the meta-transformations in a Datalog program.

## Running an experiment

The script `benchmark.py` can be used to produce the results.
