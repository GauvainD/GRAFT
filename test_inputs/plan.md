First: store all mappings (table -> table and prop -> prop)

Identify three cases: copy (1:1 table mapping), split (1:N table mapping), and
merge (N:1 table mapping)

If copy:
  Rename table name and mapped props to preserve names from source to target

If split:
  remove all prop that is not mapped in target nodes (it is a foreign key) and
create an edge between the new nodes (name: first unmapped prop by alphabetical
order). Rename all props in target to source names.

If merge:
  One prop is duplicated (with different) names in both source nodes (join
attr). It is not mapped in one of them. Add an edge. Rename all props in target to source names.
