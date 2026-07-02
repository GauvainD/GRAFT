# Goals

To define transformations between a source and a target graph database schema
(property graph).

Transformations are defined as sequences of edit operations using NextId(). The
program is soufflé datalog.

Transformations must be:
- Adding a property
- Removing a property
- Merging two nodes
- Splitting a node into two

# Files

## ./definitions.dl

Definitions of basic predicates.

## ./data.dl

Example of two schemas in datalog

## ./transfos.dl

The previous attempt that must be fixed.

## ./transfos-icij.dl

Transformations defined for the ICIJ dataset.

## ./split.dl

Unfinished implementation of a split transformation that splits a node into two
nodes.

## ./merge.dl

Merges two nodes into a single node.
