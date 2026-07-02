#!/usr/bin/env python3
"""Regenerate every figure used in the GRAFT paper from a single script.

Usage (run from ``experiments/results/``)::

    python3 make_paper_figures.py                 # build all 8 figures
    python3 make_paper_figures.py figure7         # build just one
    python3 make_paper_figures.py --outdir myout  # override output directory
    python3 make_paper_figures.py --list          # list figure names
"""

import argparse
import os
import re

import numpy as np
import pandas as pd
import polars as pl
import seaborn as sb
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default output directory.
OUTPUT_DIR = "plots"

# --- params ---
HSPACE = 0.3
TOP_LEGEND = 0.81
_LABELS = "abcdefg"


def label_block(i, label):
    """Prefix a block label with its ``(a) ``/``(b) `` ... enumerator."""
    return f"({_LABELS[i]}) {label}"


# --- CSV file paths ---
CSV_IBENCH_TRANSFOS = "./csv/transformations_ibench.csv"
CSV_SIMILARITIES = "./csv/similarities.csv"
CSV_CAND_SCHEMA = "./csv/cand-schema-comparison.csv"
CSV_CAND_SCHEMA_PATH = "./csv/cand-schema-comparison-path.csv"
CSV_NOK = "./csv/nok.csv"
CSV_SCHEMA_SIZE = "./csv/schema-size-comparison.csv"
CSV_DATASETS = "./csv/dataset-comparison.csv"
CSV_THETA = "./csv/theta-comparison.csv"
CSV_SIMILARITY_COMP = "./csv/similarity-comparison.csv"
CSV_BEAM = "./csv/beam.csv"
CSV_IBENCH_HUGE = "csv/ibench_huge.csv"
CSV_ALL_TRANSFOS_IBENCH = "./csv/all_transfos_ibench.csv"
CSV_SF = "./csv/sf_data.csv"
CSV_ICIJ_TRANSFOS = "./csv/transformations_icij.csv"

FLOAT = re.compile(r"(\d+(?:\.\d*)?)")

# --- pandas-based figures (figure 6/7/8, dblp, stackbars) ---
DATASETS = ["persondata", "dblp_to_amalgam1", "amalgam1_to_amalgam3", "flighthotel"]
DATASET_LABELS = {}

METRIC_LABELS_PRUNING = {
    "similarity": "Norm. Sim. Improv.",
    "time": "Tot. Runtime (s)",
    "total_transfo": "# Edit Op.",
    "avg_transfo_length": "Avg. Transfo. Size",
}

METRIC_LABELS_ADDITIONAL = {
    "similarity": "Norm. Sim. Improv.",
    "time": "Tot. Runtime (s)",
    "total_transfo": "# Edit Op.",
    "path": "Reuse Ratio",
    "dup": "Ratio Dup. Schemas",
    "avg_transfo_length": "Avg. Transfo. Size",
    "theta": "Req. Sim.",
    "minhash": "Sim. Precision",
}

METRIC_LABELS_INCREASING = {
    "similarity": "Norm. Sim. Improv.",
    "time": "Tot. Runtime (s)",
    "total_transfo": "# Edit Op.",
    "avg_transfo_length": "Avg. Transfo. Size",
}

AGG_FUNCS = {
    "similarity": "mean",
    "time": "mean",
    "avg_transfo_length": "mean",
    "total_transfo": "max",
}

# Strategy palette shared by every point-plot figure. Colors are keyed by
# strategy *name* (not position) so a strategy keeps its color even when some
# figures omit one of them.
STRATS = ["greedy", "naive", "random", "weighted_distance"]
STRAT_COLORS = dict(zip(STRATS, sb.color_palette("tab10", n_colors=len(STRATS))))

# --- icij_instance / instance_experiment (polars-based bar figures) ---
INIT_SIM = 0.9881542699724518

# Distinct hatch patterns so bars stay distinguishable without relying on
# color (e.g. greyscale print); cycled per hue group.
HATCHES = ["/", "\\", "x", ".", "+", "o", "*", "O", "|", "-"]

SF = ["sf1", "sf2", "sf3"]
# Scale-factor tick labels used by instance_experiment's scale-factor block.
SF_NAMES = {"sf1": "13.97M", "sf2": "28.06M", "sf3": "42.14M"}

# Every transformation gets its own color, shared across all plots. The iBench
# and ICIJ transformations take disjoint slices of the palette so the blocks
# never reuse a color. A matching hatch map pins each transformation to a fixed
# hatch by name for greyscale-print safety.
IBENCH_TRANSFOS = ["Merge", "Split", "Add_prps", "Rm_prps"]
ICIJ_TRANSFOS = ["R1", "R6", "R9", "R10", "R14"]
ALL_TRANSFOS = IBENCH_TRANSFOS + ICIJ_TRANSFOS
TRANSFO_COLORS = sb.color_palette("Paired", n_colors=len(ALL_TRANSFOS + SF))
TRANSFO_COLOR_DICT = dict(zip(ALL_TRANSFOS + SF, TRANSFO_COLORS))
TRANSFO_HATCH_DICT = {
    name: HATCHES[i % len(HATCHES)] for i, name in enumerate(ALL_TRANSFOS + SF)
}

# Binning resolution for the bar-plot x-axes.
SIM_DECIMALS = 2  # bins for the normalized similarity (top plots)
TIME_DECIMALS = 1  # bins for time in seconds (bottom plot)

NAMES = {
    "elems": "# Elems.",
    "time": "Runtime (s)",
    "sim": "Norm. Sim. Improv.",
    "sf": "Scale Factor",
    "size": "# Edit Op.",
    "transfo": "Transfo.",
}

ELEMS_SCALE = 1e5


# ---------------------------------------------------------------------------
# Small numeric helpers (pandas figures)
# ---------------------------------------------------------------------------


def convert_to_list(token):
    if pd.isna(token):
        return []
    return [float(x) for x in FLOAT.findall(str(token))]


def compute_average(token):
    elems = convert_to_list(token)
    if len(elems) > 0:
        return sum(elems) / len(elems)
    return None


def compute_avg_ratio(ser):
    e1 = convert_to_list(ser["num_dup"])
    e2 = convert_to_list(ser["num_tot"])
    if len(e1) == 0 or len(e2) == 0:
        return np.nan
    return sum(x / y for x, y in zip(e1, e2)) / len(e1)


def total_transfo(s):
    if not isinstance(s, str):
        return np.nan
    s = s.strip("'")
    # Empty cell or the bare ``None`` placeholder of a failed run: no data, so
    # return NaN to keep it off the plot rather than counting it as a point.
    if len(s) == 0 or s == "None":
        return np.nan
    total = 0
    for t in s.split(":"):
        for v in t.split(";"):
            if len(v) > 0:
                total += 1
    return total


def avg_transfo_length(s):
    if not isinstance(s, str):
        return np.nan
    s = s.strip("'")
    if len(s) == 0 or s == "None":
        return np.nan
    total = 0
    transfos = s.split(":")
    for t in transfos:
        for v in t.split(";"):
            if len(v) > 0:
                total += 1
    return total / len(transfos)


def avg_size_transfo(x):
    """Average number of ops per transformation in a ``a;b:c;d`` path string."""
    if x is None:
        return None
    transfos = x.split(":")
    ops = [len(t.split(";")) for t in transfos]
    return sum(ops) / len(ops)


# ---------------------------------------------------------------------------
# Data loading (pandas figures)
# ---------------------------------------------------------------------------


def load_one_pruning(filename):
    pruning = pd.read_csv(filename)
    new_index = pruning["index"].str.extract(r"([^-]*)-(.+)-(\d+)")
    new_index[2] = new_index[2].astype(int)
    pruning.index = pd.MultiIndex.from_frame(new_index)
    pruning = pruning.drop(columns="index")
    return pruning


def load_pruning_dataset():
    """Point-plot pruning dataset shared by figures 6/8, dblp and the beam
    figure. Reads similarities + cand-schema-comparison(-path) + nok."""
    try:
        sims = pd.read_csv(CSV_SIMILARITIES)
        sims = sims[sims["inserted"] == 0]
        sims.index = pd.Index(sims["dataset"])
        sims.index.names = ["dataset"]
        sims = sims.drop(columns=["inserted", "dataset"])
    except FileNotFoundError:
        print(f"Warning: {CSV_SIMILARITIES} not found. Creating mock data for testing.")
        sims = pd.DataFrame(
            {"similarity": [0.5]}, index=pd.Index(["mock_dataset"], name="dataset")
        )

    try:
        pruning = load_one_pruning(CSV_CAND_SCHEMA)
        prun_path = load_one_pruning(CSV_CAND_SCHEMA_PATH)
        pruning["transfo_path"] = prun_path["transfo_path"]
    except FileNotFoundError:
        print("Error: Main data files not found.")
        return pd.DataFrame()

    cols = [
        "similarity",
        "path",
        "time",
        "souffle_time",
        "neo4j_time",
        "sim_time",
        "gen_time",
        "automaton_time",
    ]

    for col in cols:
        if col in pruning.columns:
            pruning[col] = (
                pruning[col].astype(str).str.findall(r"(\d+(?:\.\d+)?(?:e-?\d+)?|None)")
            )

    pruning["transfo_path"] = pruning["transfo_path"].fillna("['', '', '', '', '']")
    # Match quoted path strings, or the bare ``None`` placeholder written for
    # failed runs (e.g. ibench_huge-naive-8/16). Without the ``|None``
    # alternative those rows yield 0 entries while the numeric columns yield 5,
    # which breaks the explode below ("columns must have matching element
    # counts"). The non-greedy ``'.*?'`` consumes whole quoted paths first, so a
    # literal "None" inside a real path is never matched on its own.
    pruning["transfo_path"] = pruning["transfo_path"].str.findall(r"'.*?'|None")

    pruning = pruning.explode(cols + ["transfo_path"])

    for col in cols:
        if col in pruning.columns:
            pruning[col] = pd.to_numeric(pruning[col], errors="coerce")

    pruning.index.names = ["dataset", "strat", "pruning"]

    pruning = pruning.join(sims["similarity"].rename("base_sim"), on="dataset")
    pruning["similarity"] = (pruning["similarity"] - pruning["base_sim"]) / (
        1 - pruning["base_sim"]
    )
    pruning = pruning.drop(columns=["base_sim"])

    pruning["total_transfo"] = pruning["transfo_path"].map(total_transfo)
    pruning["avg_transfo_length"] = pruning["transfo_path"].map(avg_transfo_length)

    nok = load_one_pruning(CSV_NOK)
    nok.index.names = ["dataset", "strat", "pruning"]

    for col in cols:
        if col in nok.columns:
            nok[col] = (
                nok[col].astype(str).str.findall(r"(\d+(?:\.\d+)?(?:e-?\d+)?|None)")
            )

    nok = nok.explode(cols)
    nok["total_transfo"] = nok["path"]

    for col in cols:
        if col in nok.columns:
            nok[col] = pd.to_numeric(nok[col], errors="coerce")

    nok["total_transfo"] = np.nan
    nok["avg_transfo_length"] = np.nan
    nok["total_transfo"] = nok["path"]
    other = pd.DataFrame(
        [[np.nan] * len(nok.columns)] * 2,
        columns=nok.columns,
        index=pd.MultiIndex.from_arrays(
            [["amalgam1_to_amalgam3"] * 2, ["weighted_distance", "naive"], [0] * 2],
            names=["dataset", "strat", "pruning"],
        ),
    )
    nok = pd.concat([nok, other])
    pruning = pd.concat([pruning, nok])

    return pruning


def load_increasing_dataset():
    """Schema-size (# added nodes) dataset used by figure 7."""
    sims = pd.read_csv(CSV_SIMILARITIES)
    sims.index = pd.MultiIndex.from_frame(sims[["dataset", "inserted"]])
    sims = sims.drop(columns=["dataset", "inserted"])

    increasing = pd.read_csv(CSV_SCHEMA_SIZE)
    new_index = increasing["index"].str.extract(r"([^-]*)-(.+)-sources-(\d+).*")
    new_index.columns = ["dataset", "strat", "inserted"]
    new_index["inserted"] = new_index["inserted"].astype(int)
    increasing.index = pd.MultiIndex.from_frame(new_index)
    increasing = increasing.drop(columns="index")
    cols = ["similarity", "path", "time"]
    increasing = increasing.drop(
        columns=["souffle_time", "neo4j_time", "sim_time", "gen_time", "automaton_time"]
    )
    for col in cols:
        increasing[col] = increasing[col].str.findall(
            r"(\d+(?:\.\d+)?(?:e-?\d+)?|None)"
        )
    increasing = increasing.explode(cols)
    increasing["run_id"] = increasing.groupby(
        level=["dataset", "strat", "inserted"]
    ).cumcount()
    increasing = increasing.set_index("run_id", append=True)
    for col in cols:
        increasing[col] = pd.to_numeric(increasing[col], errors="coerce")
    increasing["similarity"] = (
        increasing["similarity"]
        .sub(sims["similarity"], axis=0)
        .div(1 - sims["similarity"], axis=0)
    )
    mask = increasing.isna().any(axis=1).groupby(["dataset", "strat"]).transform("any")
    increasing = increasing[~mask]
    return increasing


def load_additional_data():
    """Dataset / theta / minhash datasets used by figures 7 and 8.

    Returns ``(data, theta_data, minhash_data)``.
    """
    similarities = pd.read_csv(CSV_SIMILARITIES)
    similarities = similarities[similarities["inserted"] == 0].drop("inserted", axis=1)
    similarities.index = pd.Index(similarities["dataset"])
    similarities.drop("dataset", axis=1, inplace=True)

    data = pd.read_csv(CSV_DATASETS, index_col=0)
    data.index = pd.MultiIndex.from_tuples(
        [(name, x) for name, x in data.index.str.split("-")]
    )
    data.index.names = ["strategy", "dataset"]

    def load_param_data(filename, parname):
        df = pd.read_csv(filename)
        parts = df["index"].str.rsplit("-", n=2, expand=True)
        parts.columns = ["dataset", "strategy", parname]
        df.index = pd.MultiIndex.from_frame(parts)
        df = df.drop(columns="index")
        return df

    theta_data = load_param_data(CSV_THETA, "theta")
    minhash_data = load_param_data(CSV_SIMILARITY_COMP, "minhash")
    cols = ["similarity", "time", "path"]
    for col in cols:
        for df in [data, theta_data, minhash_data]:
            df[col] = df[col].map(compute_average)
    data["dup"] = data[["num_dup", "num_tot"]].apply(compute_avg_ratio, axis=1)
    for df in [data, theta_data, minhash_data]:
        df = df.join(similarities["similarity"].rename("base_sim"), on="dataset")
        df["similarity"] = (df["similarity"] - df["base_sim"]) / (1 - df["base_sim"])
    path_data = pd.DataFrame(
        {
            "dataset": [
                "persondata",
                "dblp_to_amalgam1",
                "amalgam1_to_amalgam3",
                "flighthotel",
            ],
            "best_path": [13, 23, 24, 16],
        }
    )
    path_data = path_data.set_index(path_data["dataset"]).drop("dataset", axis=1)
    data = data.join(path_data, on="dataset")
    data["path"] = data["path"] / data["best_path"]
    return data, theta_data, minhash_data


def get_aggregated_data(df, metrics, x):
    current_agg = {m: AGG_FUNCS.get(m, "mean") for m in metrics}
    df_agg = df.groupby(["dataset", "strat", x])[metrics].agg(current_agg).reset_index()
    if metrics == ["avg_transfo_length"] and x == "pruning":
        df_agg = df_agg[df_agg[x] != 0]
    return df_agg


def get_aggregated_data_pruning(df, metrics):
    current_agg = {m: AGG_FUNCS.get(m, "mean") for m in metrics}
    return (
        df.groupby(["dataset", "strat", "pruning"])[metrics]
        .agg(current_agg)
        .reset_index()
    )


# ---------------------------------------------------------------------------
# Data loading (polars figures: icij_instance / instance_experiment)
# ---------------------------------------------------------------------------


def load_ibench_data():
    data = pl.read_csv(CSV_IBENCH_HUGE)
    data = data.with_columns(
        data["index"].str.split("-").list.to_struct(fields=["dataset", "strat", "k"])
    ).unnest("index")
    data = data.with_columns(pl.col("k").cast(pl.Int64))
    cols = [
        col
        for col in data.columns
        if col not in ["dataset", "strat", "k", "transfo_path"]
    ]

    data = data.with_columns(
        [
            pl.col(c)
            .str.strip_chars("[]")
            .str.split(",")
            .list.eval(
                pl.element()
                .str.strip_chars(" ")
                .replace("None", None)
                .cast(pl.Float64, strict=False)
            )
            for c in cols
        ]
        + [
            pl.col("transfo_path")
            .str.strip_chars("[]")
            .str.extract_all(r"'[^']+'|None")
            .list.eval(
                pl.element()
                .str.strip_chars("'")
                .str.strip_chars(" ")
                .replace("None", None)
                .map_elements(avg_size_transfo)
            )
        ]
    ).explode(cols + ["transfo_path"])
    data = data.group_by(["dataset", "strat", "k"]).mean()
    data = data.with_columns(
        ((pl.col("similarity") - INIT_SIM) / (1 - INIT_SIM)).alias("similarity")
    )
    data = data.sort(by=["strat"])
    return data


def load_transfo_data():
    """Per-transformation data for the large-scale iBench bars (all_transfos)."""
    return (
        pl.read_csv(CSV_ALL_TRANSFOS_IBENCH)
        .with_columns(
            (pl.col("sim") - INIT_SIM) / (1 - INIT_SIM), (pl.col("time") / 1000)
        )
        .with_columns(
            # Bin the continuous x values so barplot doesn't create one
            # category per unique value.
            pl.col("sim").round(SIM_DECIMALS)
        )
    )


def load_sf_data():
    return pl.read_csv(CSV_SF).with_columns(
        (pl.col("time") / 1000).round(TIME_DECIMALS)
    )


def load_icij_transfo_data():
    transfo_data = pl.read_csv(CSV_ICIJ_TRANSFOS)
    init_sim = transfo_data.filter(pl.col("transfo") == "start")["sim"][0]
    transfo_data = transfo_data.filter(pl.col("transfo") != "start")
    transfo_data = transfo_data.group_by("type").agg(
        transfo=pl.col("type").first(),
        elems=pl.col("elems").sum(),
        sim=(pl.col("sim") - init_sim).sum() / (1 - init_sim),
        time=pl.col("time").sum(),
        size=pl.col("size").sum(),
    )
    transfo_data = transfo_data.with_columns(
        (pl.col("time") / 1000).ceil().cast(pl.Int32),
        (pl.col("elems") / 100000).round(2),
    ).with_columns(
        # Bin the continuous x values so barplot doesn't create one
        # category per unique value.
        pl.col("sim").round(3)
    )
    return transfo_data


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------


def setup_grid_layout(
    outer_items,
    inner_items,
    inner_shape=(2, 2),
    header_in=0.8,
    row_in=1.2,
    hspace_floor=0.0,
    bottom=0.12,
):
    """Build an ``n_outer x (rows*cols)`` grid of axes.

    A fixed-height header band (in inches) is reserved on top of the plot area
    so the stacked legend + block title always have room regardless of plot
    height.
    """
    n_outer = len(outer_items)
    rows, cols = inner_shape

    fig_width = 4 * cols * n_outer
    fig_height = row_in * rows + header_in
    fig = plt.figure(figsize=(fig_width, fig_height))

    # Reserve the header band for the per-block legends and block titles
    # (stacked, legend on top) so neither overlaps the plots.
    top = 1 - header_in / fig_height
    outer_grid = gridspec.GridSpec(
        1, n_outer, figure=fig, wspace=0.1, top=top, bottom=bottom
    )

    axes_structure = []
    for i in range(n_outer):
        inner_grid = outer_grid[i].subgridspec(
            rows, cols, wspace=0.2, hspace=max(HSPACE, hspace_floor)
        )

        block_axes = []
        for r in range(rows):
            for c in range(cols):
                idx = r * cols + c
                if idx < len(inner_items):
                    block_axes.append(fig.add_subplot(inner_grid[r, c]))
                else:
                    block_axes.append(None)
        axes_structure.append(block_axes)

    return fig, axes_structure


def scale_elems_axis(ax):
    """Show elems in units of 1e5 via tick labels instead of the ``1e6`` offset
    text matplotlib puts above the axis (it pushes the title up and misaligns it
    with neighboring plots)."""
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v / ELEMS_SCALE:g}"))


def apply_hatches(ax, labels=None, hatch_offset=0, hatch_map=None):
    """Overlay one hatch per hue group on a barplot and return matching
    color+hatch ``Patch`` (handles, labels) for a compact, print-friendly
    legend.

    Pass ``labels`` (in hue order) when the plot carries no legend artists of
    its own -- e.g. when hue duplicates the x encoding. Pass ``hatch_map`` to pin
    each label to a fixed hatch by name (so the same category renders
    identically across every plot); otherwise ``hatch_offset`` shifts the cycled
    hatch sequence so blocks reusing the same palette never share a
    combination."""
    handles, found_labels = ax.get_legend_handles_labels()
    if labels is None:
        labels = found_labels
        colors = [h.get_facecolor() for h in handles]
    else:
        colors = [c.patches[0].get_facecolor() for c in ax.containers]
    if hatch_map is not None:
        hatches = [hatch_map[l] for l in labels]
    else:
        hatches = [
            HATCHES[(hatch_offset + i) % len(HATCHES)] for i in range(len(colors))
        ]
    for container, hatch in zip(ax.containers, hatches):
        for bar in container:
            bar.set_hatch(hatch)
            # Seaborn bars default to a transparent edge, and matplotlib draws
            # the hatch in the edge color -- so without an opaque edge the hatch
            # is invisible on the bars (while the legend swatches, which set
            # edgecolor explicitly, show it). Match them.
            bar.set_edgecolor("black")
    leg_handles = [
        Patch(facecolor=c, edgecolor="black", hatch=hatch, label=l)
        for c, l, hatch in zip(colors, labels, hatches)
    ]
    return leg_handles, labels


def block_center_x(block_axes):
    """Horizontal center (figure fraction) of a 2-column block, using the
    top-left and top-right axes as the span."""
    x0 = block_axes[0].get_position().x0
    x1 = block_axes[1].get_position().x1
    return (x0 + x1) / 2


def add_block_title(figure, block_axes, text, y=0.86, va="center"):
    """Bold block title centered on the block. Defaults to the reserved top
    margin; pass ``y`` to place a row title elsewhere (e.g. the gap between two
    rows) and ``va`` to anchor it by its bottom/top edge instead of its
    center."""
    return figure.text(
        block_center_x(block_axes),
        y,
        text,
        ha="center",
        va=va,
        fontsize=14,
        fontweight="bold",
    )


def add_block_legend(
    figure, block_axes, handles, labels, title, ncol, y=0.99, loc="upper center"
):
    """Per-block horizontal legend centered above the block. Defaults to the
    reserved top margin; pass ``y`` to place a row legend elsewhere and ``loc``
    to anchor it by its lower edge (so it grows upward)."""
    return figure.legend(
        handles,
        labels,
        loc=loc,
        bbox_to_anchor=(block_center_x(block_axes), y),
        ncol=ncol,
        frameon=False,
        title=title,
        columnspacing=1.2,
        handletextpad=0.5,
    )


def place_row_block(
    figure, block_axes, handles, labels, legend_title, block_title, ncol, row=(2, 3)
):
    """Stack a block legend (on top) and block title (below) just above the
    subplot titles of the given row. Measures rendered extents so the title
    always clears those subplot titles and the legend always clears the title --
    no fragile fractional guesses about the available band height. Works for the
    header band (row=(0, 1)) and the inter-row gap (row=(2, 3)) alike."""
    pad = 0.005  # vertical breathing room (figure fraction)
    figure.canvas.draw()
    r = figure.canvas.get_renderer()
    h = figure.bbox.height
    # Top edge of the row's subplot titles, in figure fraction.
    titles_top = max(block_axes[i].title.get_window_extent(r).y1 for i in row) / h
    # Block title sits just above those subplot titles, anchored by its bottom.
    title_artist = add_block_title(
        figure, block_axes, block_title, y=titles_top + pad, va="bottom"
    )
    # Measure the title, then stack the legend (anchored by its lower edge) above.
    figure.canvas.draw()
    title_top = title_artist.get_window_extent(r).y1 / h
    if handles is not None and labels is not None:
        add_block_legend(
            figure,
            block_axes,
            handles,
            labels,
            legend_title,
            ncol=ncol,
            y=title_top + pad,
            loc="lower center",
        )


# ---------------------------------------------------------------------------
# figure6_beam: subfigure layout helpers
# ---------------------------------------------------------------------------

# Header band (inches) reserved at the top of each subfigure for block titles
# (and, for the top subfigure, the shared legend).
BEAM_HEADER_IN = 0.9
BEAM_ROW_IN = 1.2

BEAM_METRICS = ["similarity", "time", "path"]
BEAM_DATASETS = [
    "persondata",
    "dblp_to_amalgam1",
    "amalgam1_to_amalgam3",
    "flighthotel",
]


def _load_sim_norm():
    return (
        pl.read_csv(CSV_SIMILARITIES).filter(pl.col("inserted") == 0).drop("inserted")
    )


def load_beam(filename=CSV_BEAM):
    """Beam-search dataset (beam width x strategy) for the bottom of figure6_beam."""
    df = pl.read_csv(filename).select(["index"] + BEAM_METRICS)
    df = df.with_columns(
        pl.col("index")
        .str.split("-")
        .list.to_struct(fields=["dataset", "strat", "beam"])
    ).unnest("index")
    df = df.with_columns(
        pl.col(col)
        .str.extract_all(r"\d+(?:\.\d+)?")
        .cast(pl.List(pl.Float64))
        .list.mean()
        for col in BEAM_METRICS
    )
    sims = _load_sim_norm()
    df = (
        df.join(sims, on=["dataset"], suffix="_norm")
        .with_columns(
            similarity=(pl.col("similarity") - pl.col("similarity_norm"))
            / (1 - pl.col("similarity_norm"))
        )
        .drop("similarity_norm")
    )
    return df.with_columns(pl.col("beam").cast(pl.Int64)).sort("beam")


def setup_grid_into(
    parent,
    n_outer,
    inner_items,
    subfig_h,
    inner_shape=(2, 2),
    header_in=BEAM_HEADER_IN,
    bottom=0.12,
):
    """Build an ``n_outer x (rows*cols)`` grid of axes inside a parent
    (sub)figure.

    Same layout logic as ``setup_grid_layout``, but draws into an existing
    matplotlib figure/subfigure instead of creating a new figure. The top
    ``header_in`` inches of the (sub)figure are reserved for the block titles
    and (top subfigure only) the shared legend."""
    rows, cols = inner_shape
    top = 1 - header_in / subfig_h
    outer_grid = parent.add_gridspec(1, n_outer, wspace=0.1, top=top, bottom=bottom)

    axes_structure = []
    for i in range(n_outer):
        inner_grid = outer_grid[i].subgridspec(
            rows, cols, wspace=0.2, hspace=max(HSPACE, 0)
        )
        block_axes = []
        for r in range(rows):
            for c in range(cols):
                idx = r * cols + c
                if idx < len(inner_items):
                    block_axes.append(parent.add_subplot(inner_grid[r, c]))
                else:
                    block_axes.append(None)
        axes_structure.append(block_axes)
    return axes_structure


def draw_pruning(parent, df, subfig_h, label_offset=0, bottom=0.12):
    """Draw the pruning blocks (figure 6) into ``parent``; return legend
    handles."""
    datasets = ["persondata", "ibench_huge", "amalgam1_to_amalgam3", "flighthotel"]
    metrics_to_plot = ["similarity", "time", "total_transfo"]

    df_agg = get_aggregated_data_pruning(df, metrics_to_plot)
    axes_matrix = setup_grid_into(
        parent, len(metrics_to_plot), datasets, subfig_h, bottom=bottom
    )

    legend_handles, legend_labels = [], []

    pruning_order = [p for p in sorted(df_agg["pruning"].unique())]
    if 0 in pruning_order:
        pruning_order.remove(0)
        pruning_order = pruning_order + [0]
    pos_map = {p: i for i, p in enumerate(pruning_order)}
    tick_labels = [str(p) if p != 0 else "u" for p in pruning_order]

    for block_idx, metric in enumerate(metrics_to_plot):
        block_axes = axes_matrix[block_idx]
        metric_name = METRIC_LABELS_PRUNING.get(metric, metric)

        if block_axes[0]:
            block_axes[0].annotate(
                label_block(block_idx + label_offset, metric_name),
                xy=(1.1, 1.3),
                xycoords="axes fraction",
                ha="center",
                fontsize=14,
                fontweight="bold",
            )

        for item_idx, dataset in enumerate(datasets):
            ax = block_axes[item_idx]
            if ax is None:
                continue

            subset = df_agg[df_agg["dataset"] == dataset]
            subset = subset.sort_values(
                ["dataset", "strat", "pruning"], key=lambda x: x.map(pos_map)
            )
            subset["pruning"] = pd.Categorical(subset["pruning"])

            sb.pointplot(
                data=subset,
                x="pruning",
                y=metric,
                hue="strat",
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette=STRAT_COLORS,
                ax=ax,
                scale=0.8,
                order=pos_map,
            )

            ax.set_title(DATASET_LABELS.get(dataset, dataset), fontsize=11)
            ax.set_xlabel("")
            ax.set_ylabel("")
            ax.set_xticks(range(len(pruning_order)))
            ax.set_xticklabels(tick_labels)
            for lbl in ax.get_xticklabels():
                if lbl.get_text() == "u":
                    lbl.set_color("blue")
                    lbl.set_weight("bold")

            if item_idx < 2:
                plt.setp(ax.get_xticklabels(), visible=False)
            else:
                ax.set_xlabel("# cand. schema")

            if not legend_handles and ax.get_legend_handles_labels()[0]:
                h, l = ax.get_legend_handles_labels()
                legend_handles.extend(h)
                legend_labels.extend(l)

            if ax.get_legend():
                ax.get_legend().remove()

    return legend_handles, legend_labels


def draw_beam(parent, df, subfig_h, label_offset=0, header_in=BEAM_HEADER_IN):
    """Draw the beam blocks into ``parent``; return legend handles/labels."""
    axes_matrix = setup_grid_into(
        parent, len(BEAM_METRICS), BEAM_DATASETS, subfig_h, header_in=header_in
    )

    legend_handles, legend_labels = [], []

    for block_idx, metric in enumerate(BEAM_METRICS):
        block_axes = axes_matrix[block_idx]

        for item_idx, dataset in enumerate(BEAM_DATASETS):
            ax = block_axes[item_idx]
            if dataset == "dblp_to_amalgam1":
                dataset = "ibench_huge"
            subset = df.filter(pl.col("dataset") == dataset)

            sb.pointplot(
                data=subset,
                x="beam",
                y=metric,
                hue="strat",
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette=STRAT_COLORS,
                ax=ax,
                scale=0.8,
            )

            ax.set_title(DATASET_LABELS.get(dataset, dataset), fontsize=11)
            ax.set_xlabel("")
            ax.set_ylabel("")

            if item_idx < 2:
                plt.setp(ax.get_xticklabels(), visible=False)
            else:
                ax.set_xlabel("beam width")

            if not legend_handles and ax.get_legend_handles_labels()[0]:
                legend_handles, legend_labels = ax.get_legend_handles_labels()
            if ax.get_legend():
                ax.get_legend().remove()

    return legend_handles, legend_labels


# ---------------------------------------------------------------------------
# Figure builders (one per output PDF)
# ---------------------------------------------------------------------------


def make_transformation_details(outdir):
    """transformation_details.pdf -- per-op impacted elements + runtime bars."""
    NUM_ELEMS = 84280 * 500 + 84280
    data = pl.read_csv(CSV_IBENCH_TRANSFOS)
    data = data.with_columns(pl.col("num_elems").cast(pl.Float64) / NUM_ELEMS)

    data = data.group_by("operation").agg(
        pl.col("num_elems").mean(),
        pl.col("time").mean(),
    )
    # Rename operations for display (add_prop -> copy_prop, create_prop -> add_prop).
    data = data.with_columns(
        operation=pl.when(pl.col("operation") == "add_prop")
        .then(pl.lit("copy_prop"))
        .otherwise(pl.col("operation"))
    )
    data = data.with_columns(
        operation=pl.when(pl.col("operation") == "create_prop")
        .then(pl.lit("add_prop"))
        .otherwise(pl.col("operation"))
    )

    hist_order = [
        "del_prop",
        "del_node",
        "add_node",
        "copy_prop",
        "add_edge",
        "add_prop",
    ]
    data = data.sort(
        pl.col("operation").replace_strict(
            {op: i for i, op in enumerate(hist_order)}, default=len(hist_order)
        )
    )

    fig, ax = plt.subplots(1, 2, figsize=(12, 4), layout="constrained", sharey=True)
    sb.barplot(data, y="operation", x="num_elems", orient="h", ax=ax[0]).set(
        xlabel="Average percentage of impacted elements (nodes and relationships)",
        ylabel="Op type",
    )
    sb.barplot(data, y="operation", x="time", orient="h", ax=ax[1]).set(
        xlabel="Average runtime of queries (ms)", ylabel="Op type"
    )

    _save(fig, outdir, "transformation_details.pdf", tight=False)


def make_figure7(outdir):
    """figure7.pdf -- theta sweep + increasing schema size (sim & time)."""
    _, theta_data, _ = load_additional_data()
    inc_df = load_increasing_dataset()

    inner_items = DATASETS
    outer_blocks = [
        (theta_data, "similarity", "theta", METRIC_LABELS_ADDITIONAL["theta"]),
        (inc_df, "similarity", "inserted", "# added nodes"),
        (inc_df, "time", "inserted", "# added nodes"),
    ]

    fig, axes_matrix = setup_grid_layout(
        outer_blocks,
        inner_items,
        inner_shape=(2, 2),
        header_in=0.8,
        row_in=1.2,
        hspace_floor=0.0,
        bottom=0.12,
    )

    legend_handles = []
    legend_labels = []

    for block_idx, (df, metric, x, x_label) in enumerate(outer_blocks):
        if block_idx == 0:
            df_agg = df.reset_index()
            df_agg["theta"] = df_agg["theta"].map(
                {x: str(round(1 - float(x), 2)) for x in ["0.5", "0.7", "0.9", "1.0"]}
            )
        else:
            df_agg = get_aggregated_data(df, [metric], x)
        block_axes = axes_matrix[block_idx]
        metric_name = METRIC_LABELS_INCREASING.get(metric, metric)

        if block_axes[0]:
            block_axes[0].annotate(
                label_block(block_idx, metric_name),
                xy=(1.1, 1.3),
                xycoords="axes fraction",
                ha="center",
                fontsize=14,
                fontweight="bold",
            )

        for item_idx, dataset in enumerate(inner_items):
            ax = block_axes[item_idx]
            if ax is None:
                continue

            subset = df_agg[df_agg["dataset"] == dataset]
            hue = "strategy" if block_idx == 0 else "strat"

            sb.pointplot(
                data=subset,
                x=x,
                y=metric,
                hue=hue,
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette=STRAT_COLORS,
                ax=ax,
                scale=0.8,
            )

            ax.set_title(DATASET_LABELS.get(dataset, dataset), fontsize=11)
            ax.set_xlabel("")
            ax.set_ylabel("")

            if item_idx < 2:
                plt.setp(ax.get_xticklabels(), visible=False)
            else:
                ax.set_xlabel(x_label)

            if not legend_handles and ax.get_legend_handles_labels()[0]:
                h, l = ax.get_legend_handles_labels()
                legend_handles.extend(h)
                legend_labels.extend(l)

            if ax.get_legend():
                ax.get_legend().remove()

    if legend_handles:
        fig.legend(
            legend_handles,
            legend_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.05),
            ncol=len(legend_labels),
            title="Strategy",
            frameon=False,
            fontsize=12,
        )

    sb.despine()
    plt.tight_layout()
    plt.subplots_adjust(top=TOP_LEGEND)
    _save(fig, outdir, "figure7.pdf")


def make_figure8(outdir):
    """figure8.pdf -- avg transfo size (pruning), per-dataset metrics, minhash."""
    data, _, minhash_data = load_additional_data()
    prun_df = load_pruning_dataset()

    inner_items = DATASETS
    outer_blocks = [
        (prun_df, "avg_transfo_length", "pruning", "# cand. schema"),
        (data, "dataset", "dataset", "dataset"),
        (minhash_data, "similarity", "minhash", METRIC_LABELS_ADDITIONAL["minhash"]),
    ]

    fig, axes_matrix = setup_grid_layout(
        outer_blocks,
        inner_items,
        inner_shape=(2, 2),
        header_in=0.8,
        row_in=1.2,
        hspace_floor=0.0,
        bottom=0.12,
    )

    legend_handles = []
    legend_labels = []

    for block_idx, (df, metric, x, x_label) in enumerate(outer_blocks):
        if block_idx == 0:
            df_agg = get_aggregated_data(df, [metric], x)
        else:
            df_agg = df.reset_index()
        block_axes = axes_matrix[block_idx]
        metric_name = METRIC_LABELS_ADDITIONAL.get(metric, metric)
        if block_idx == 1:
            dataset_map = {
                "persondata": "D1",
                "dblp_to_amalgam1": "D2",
                "amalgam1_to_amalgam3": "D3",
                "flighthotel": "D4",
            }
            df_agg["dataset"] = df_agg["dataset"].map(dataset_map)
            block_axes[0].annotate(
                label_block(block_idx, "Datasets"),
                xy=(1.1, 1.3),
                xycoords="axes fraction",
                ha="center",
                fontsize=14,
                fontweight="bold",
            )
            for index, metric in enumerate(["similarity", "time", "path", "dup"]):
                ax = block_axes[index]
                sb.pointplot(
                    data=df_agg,
                    x=x,
                    y=metric,
                    hue="strategy",
                    dodge=0.2,
                    markers="o",
                    linestyles="-",
                    errorbar=None,
                    palette=STRAT_COLORS,
                    ax=ax,
                    scale=0.8,
                    order=["D1", "D2", "D3", "D4"],
                )

                ax.set_title(METRIC_LABELS_ADDITIONAL[metric], fontsize=11)
                ax.set_xlabel("")
                ax.set_ylabel("")

                if index < 2:
                    plt.setp(ax.get_xticklabels(), visible=False)
                else:
                    ax.set_xlabel(x_label)

                if not legend_handles and ax.get_legend_handles_labels()[0]:
                    h, l = ax.get_legend_handles_labels()
                    legend_handles.extend(h)
                    legend_labels.extend(l)

                if ax.get_legend():
                    ax.get_legend().remove()
            continue

        if block_axes[0]:
            block_axes[0].annotate(
                label_block(block_idx, metric_name),
                xy=(1.1, 1.3),
                xycoords="axes fraction",
                ha="center",
                fontsize=14,
                fontweight="bold",
            )
        if block_idx == 2:
            df_agg["minhash"] = df_agg["minhash"].map(
                {
                    "None": "Jaccard",
                    "10": "10",
                    "50": "50",
                    "100": "100",
                    "200": "200",
                    "weighted": "w",
                }
            )

        for item_idx, dataset in enumerate(inner_items):
            ax = block_axes[item_idx]
            if ax is None:
                continue

            lookup_dataset = dataset
            if block_idx == 0 and dataset == "dblp_to_amalgam1":
                dataset = "ibench_huge"
                lookup_dataset = "ibench_huge"
            subset = df_agg[df_agg["dataset"] == lookup_dataset]
            hue = "strat" if block_idx == 0 else "strategy"

            sb.pointplot(
                data=subset,
                x=x,
                y=metric,
                hue=hue,
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette=STRAT_COLORS,
                ax=ax,
                scale=0.8,
            )

            ax.set_title(DATASET_LABELS.get(dataset, dataset), fontsize=11)
            ax.set_xlabel("")
            ax.set_ylabel("")
            for lbl in ax.get_xticklabels():
                if lbl.get_text() == "w":
                    lbl.set_color("blue")
                    lbl.set_weight("bold")

            if item_idx < 2:
                plt.setp(ax.get_xticklabels(), visible=False)
            else:
                ax.set_xlabel(x_label)

            if not legend_handles and ax.get_legend_handles_labels()[0]:
                h, l = ax.get_legend_handles_labels()
                legend_handles.extend(h)
                legend_labels.extend(l)

            if ax.get_legend():
                ax.get_legend().remove()

    order = [3, 0, 1, 2]
    if legend_handles:
        fig.legend(
            [legend_handles[id] for id in order],
            [legend_labels[id] for id in order],
            loc="upper center",
            bbox_to_anchor=(0.5, 1.05),
            ncol=len(legend_labels),
            title="Strategy",
            frameon=False,
            fontsize=12,
        )

    sb.despine()
    plt.tight_layout()
    plt.subplots_adjust(top=TOP_LEGEND)
    _save(fig, outdir, "figure8.pdf")


def make_alt_stackbars(outdir):
    """alt_stackbars.pdf -- GRAFT runtime breakdown (stacked horizontal bars).

    Original name: figure9.pdf.
    """
    cols = ["souffle_time", "neo4j_time", "sim_time", "gen_time", "automaton_time"]
    col_names = {
        "souffle_time": "Edit. op. gen.",
        "neo4j_time": "Metagraph const.",
        "sim_time": "Sim. comp.",
        "gen_time": "Schema gen.",
        "automaton_time": "Transfo. gen.",
    }
    datasets = DATASETS
    dataset_names = {ds: f"D{i + 1}" for i, ds in enumerate(datasets)}
    colors = sb.color_palette("tab10", n_colors=len(cols))

    data = pd.read_csv(CSV_DATASETS)
    data[["strat", "dataset"]] = data["index"].str.split("-", expand=True)
    data = data[data["strat"] == "greedy"]
    data = data[cols + ["dataset"]]

    for col in cols:
        data[col] = data[col].str.findall(r"\d+\.\d+")
    data = data.explode(cols)
    for col in cols:
        data[col] = pd.to_numeric(data[col])
    data = data.groupby(["dataset"]).mean().reset_index()

    bottom = np.zeros(len(datasets))
    x_labels = [dataset_names[ds] for ds in datasets]

    fig, ax = plt.subplots(figsize=(7.25, 2))

    for i, col in enumerate(cols):
        values = data[col].to_numpy()
        ax.barh(
            x_labels,
            values,
            left=bottom,
            color=colors[i],
            edgecolor="black",
            label=col_names[col],
        )
        bottom += values

    ax.set_xscale("log")
    ax.set_xlabel("Time (s)")

    fig.legend(
        [col_names[col] for col in cols],
        ncols=5,
        loc="upper center",
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(0.5, 1.1),
    )
    plt.tight_layout()
    _save(fig, outdir, "alt_stackbars.pdf", dpi=300)


def make_dblp_pruning(outdir):
    """dblp_pruning.pdf -- single-dataset (dblp_to_amalgam1) pruning sweep."""
    pruning_data = load_pruning_dataset()

    metrics = ["similarity", "time", "total_transfo"]
    fig, axes_matrix = setup_grid_layout(
        ["dblp_to_amalgam1"],
        metrics,
        inner_shape=(2, 2),
        header_in=0.8,
        row_in=1.4,
        hspace_floor=1.1,
        bottom=0.12,
    )
    axes = axes_matrix[0]

    df_agg = get_aggregated_data_pruning(pruning_data, metrics)
    pruning_order = [p for p in sorted(df_agg["pruning"].unique())]
    if 0 in pruning_order:
        pruning_order.remove(0)
        pruning_order = pruning_order + [0]
    pos_map = {p: i for i, p in enumerate(pruning_order)}
    tick_labels = [str(p) if p != 0 else "u" for p in pruning_order]

    subset = df_agg[df_agg["dataset"] == "dblp_to_amalgam1"]

    for idx, metric in enumerate(metrics):
        sns_ax = sb.pointplot(
            data=subset,
            x="pruning",
            y=metric,
            hue="strat",
            dodge=0.4,
            markers="o",
            linestyles="-",
            errorbar=None,
            palette=STRAT_COLORS,
            ax=axes[idx],
            scale=0.8,
            order=pos_map,
        )
        axes[idx].set_title(
            f"({idx + 1}) {METRIC_LABELS_ADDITIONAL[metric]}", fontsize=11
        )
        axes[idx].set_xlabel("# cand. schema")
        axes[idx].set_ylabel(METRIC_LABELS_ADDITIONAL[metric])
        axes[idx].set_xticks(range(len(pruning_order)))
        axes[idx].set_xticklabels(tick_labels)
        for lbl in axes[idx].get_xticklabels():
            if lbl.get_text() == "u":
                lbl.set_color("blue")
                lbl.set_weight("bold")
        sns_ax.get_legend().remove()

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.05),
        ncol=len(labels),
        title="Strategy",
        frameon=False,
        fontsize=12,
    )
    sb.despine()
    _save(fig, outdir, "dblp_pruning.pdf")


def make_figure6_beam(outdir):
    """figure6_beam.pdf -- pruning blocks (top) stacked over beam blocks (bottom)."""
    pruning_df = load_pruning_dataset()
    beam_df = load_beam()

    rows, cols = 2, 2
    n_outer = 3
    fig_width = 4 * cols * n_outer
    subfig_h = BEAM_ROW_IN * rows + BEAM_HEADER_IN
    fig = plt.figure(figsize=(fig_width, 2 * subfig_h))

    top, bottom = fig.subfigures(2, 1, hspace=0.0)

    # Top: pruning (figure 6) -> blocks (a)(b)(c). Bottom: beam -> (d)(e)(f).
    # Tighten the facing edges: shrink the top half's bottom margin and the
    # bottom half's header band so the two halves sit closer together.
    handles, labels = draw_pruning(
        top, pruning_df, subfig_h, label_offset=0, bottom=0.07
    )
    beam_handles, beam_labels = draw_beam(
        bottom, beam_df, subfig_h, label_offset=3, header_in=0.45
    )

    # Both halves share the strategy palette, so one legend suffices. Fall back
    # to the beam handles if the pruning half produced none.
    if not handles:
        handles, labels = beam_handles, beam_labels

    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.01),
            ncol=len(labels),
            title="Strategy",
            frameon=False,
            fontsize=12,
        )

    sb.despine()
    _save(fig, outdir, "figure6_beam.pdf")


def make_icij_instance(outdir):
    """icij_instance.pdf -- ICIJ instance transformation (runtime & #elems bars)."""
    fig, axes = setup_grid_layout(
        [0],
        [0] * 2,
        (1, 2),
        header_in=0.7,
        row_in=1.9,
        hspace_floor=1.2,
        bottom=0.2,
    )
    block_axes = axes[0]

    transfo_data = load_icij_transfo_data()

    r = sb.barplot(
        transfo_data,
        x="sim",
        y="time",
        hue="transfo",
        ax=block_axes[0],
        palette=TRANSFO_COLOR_DICT,
    )
    r.set(xlabel="Norm. Sim. Improv.", ylabel=NAMES["time"])
    # Capture the transfo handles for the shared block legend before removing.
    ibench_handles, ibench_labels = apply_hatches(r, hatch_map=TRANSFO_HATCH_DICT)
    r.get_legend().remove()

    r = sb.barplot(
        transfo_data,
        x="sim",
        y="elems",
        hue="transfo",
        ax=block_axes[1],
        palette=TRANSFO_COLOR_DICT,
    )
    r.set(xlabel="Norm. Sim. Improv.", ylabel=f"{NAMES['elems']} ($\\times 10^5$)")

    apply_hatches(r, hatch_map=TRANSFO_HATCH_DICT)
    r.get_legend().remove()

    place_row_block(
        fig,
        block_axes,
        ibench_handles,
        ibench_labels,
        "Transformation",
        "",
        ncol=len(ibench_labels),
        row=(0, 1),
    )

    sb.despine()
    _save(fig, outdir, "icij_instance.pdf", tight=False)


def make_instance_experiment(outdir):
    """instance_experiment.pdf -- scaled iBench (top) + large-scale iBench (bottom)."""
    fig, axes = setup_grid_layout(
        [0],
        [0] * 4,
        (2, 2),
        header_in=0.4,
        row_in=1.9,
        hspace_floor=1.2,
        bottom=0.1,
    )
    block_axes = axes[0]

    # --- Top row: scale-factor bars ---
    sf_data = load_sf_data()
    r = sb.barplot(sf_data, x="sf", y="time", ax=block_axes[0])
    r.set(xlabel=NAMES["sf"], ylabel=f"{NAMES['time']}")
    r.set_xticks(r.get_xticks())
    r.set_xticklabels(
        [SF_NAMES.get(t.get_text(), t.get_text()) for t in r.get_xticklabels()]
    )
    if r.get_legend() is not None:
        r.get_legend().remove()

    r = sb.barplot(sf_data, x="sf", y="elems", ax=block_axes[1])
    r.set(xlabel=NAMES["sf"], ylabel=f"{NAMES['elems']} ($\\times 10^5$)")
    r.set_xticks(r.get_xticks())
    r.set_xticklabels(
        [SF_NAMES.get(t.get_text(), t.get_text()) for t in r.get_xticklabels()]
    )
    scale_elems_axis(r)
    if r.get_legend() is not None:
        r.get_legend().remove()

    place_row_block(
        fig,
        block_axes,
        None,
        None,
        NAMES["sf"],
        label_block(0, "Scaled instance transformation of large scale iBench"),
        ncol=0,
        row=(0, 1),
    )

    # --- Bottom row: large-scale iBench transformation bars ---
    transfo_data = load_transfo_data()
    r = sb.barplot(
        transfo_data,
        x="sim",
        y="time",
        hue="transfo",
        ax=block_axes[2],
        palette=TRANSFO_COLOR_DICT,
    )
    r.set(xlabel="Norm. Sim. Improv.", ylabel=NAMES["time"])
    ibench_handles, ibench_labels = apply_hatches(r, hatch_map=TRANSFO_HATCH_DICT)
    r.get_legend().remove()

    r = sb.barplot(
        transfo_data,
        x="sim",
        y="elems",
        hue="transfo",
        ax=block_axes[3],
        palette=TRANSFO_COLOR_DICT,
    )
    r.set(xlabel="Norm. Sim. Improv.", ylabel=f"{NAMES['elems']} ($\\times 10^5$)")
    scale_elems_axis(r)
    apply_hatches(r, hatch_map=TRANSFO_HATCH_DICT)
    r.get_legend().remove()

    place_row_block(
        fig,
        block_axes,
        ibench_handles,
        ibench_labels,
        "Transformation",
        label_block(1, "Instance transformation of large scale iBench"),
        ncol=len(ibench_labels),
        row=(2, 3),
    )

    sb.despine()
    # Original used a plain savefig (no tight bbox): keep the exact page size.
    _save(fig, outdir, "instance_experiment.pdf", tight=False)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# Ordered so shared/faster figures come first; keys are the CLI names.
BUILDERS = {
    "transformation_details": make_transformation_details,
    "figure7": make_figure7,
    "figure8": make_figure8,
    "alt_stackbars": make_alt_stackbars,
    "dblp_pruning": make_dblp_pruning,
    "figure6_beam": make_figure6_beam,
    "icij_instance": make_icij_instance,
    "instance_experiment": make_instance_experiment,
}


def _save(fig, outdir, filename, dpi=None, tight=True):
    """Save ``fig`` to ``outdir/filename`` and close it so pyplot state never
    leaks into the next figure.
    """
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, filename)
    kwargs = {}
    if dpi is not None:
        kwargs["dpi"] = dpi
    if tight:
        kwargs["bbox_inches"] = "tight"
    fig.savefig(path, **kwargs)
    print(f"Plot saved to {path}")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Regenerate the GRAFT paper figures.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "figure",
        nargs="?",
        choices=sorted(BUILDERS),
        help="build just this figure (default: all)",
    )
    parser.add_argument(
        "--outdir", default=OUTPUT_DIR, help=f"output directory (default: {OUTPUT_DIR})"
    )
    parser.add_argument(
        "--list", action="store_true", help="list figure names and exit"
    )
    args = parser.parse_args()

    if args.list:
        for name in BUILDERS:
            print(name)
        return

    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)

    todo = [args.figure] if args.figure else list(BUILDERS)
    for name in todo:
        print(f"Generating {name} ...")
        try:
            BUILDERS[name](args.outdir)
        except Exception as e:  # keep going so one failure doesn't block the rest
            print(f"  ERROR building {name}: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
