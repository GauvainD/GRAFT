"""
This script exists because iBench cannot generate all the required entries for each 500 node types.
"""

import os

INDIR = "out"
OUTDIR = "sf2"
NUM = 84280
NUM = 27942
NUM = 56111

if not os.path.exists(OUTDIR):
    os.mkdir(OUTDIR)

for filename in os.listdir(INDIR):
    if filename.endswith("csv"):
        print(filename)
        with (
            open(os.path.join(INDIR, filename)) as infile,
            open(os.path.join(OUTDIR, filename), "w") as outfile,
        ):
            num_attrs = len(infile.readline().strip().split("|"))
            for v in range(1, NUM + 1):
                line = "|".join([str(v)] * num_attrs)
                outfile.write(line + "\n")
