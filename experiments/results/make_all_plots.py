import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import re
import params
import os

idx = pd.IndexSlice

FLOAT = re.compile(r"(\d+(?:\.\d*)?)")

OUTPUT_DIR = "plots"

DATASETS = ["persondata", "dblp_to_amalgam1", "amalgam1_to_amalgam3", "flighthotel"]

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

DATASET_LABELS = {}

# CSV file paths
CSV_SIMILARITIES = "./csv/similarities.csv"
CSV_CAND_SCHEMA = "./csv/cand-schema-comparison.csv"
CSV_CAND_SCHEMA_PATH = "./csv/cand-schema-comparison-path.csv"
CSV_SCHEMA_SIZE = "./csv/schema-size-comparison.csv"
CSV_DATASETS = "./csv/dataset-comparison.csv"
CSV_THETA = "./csv/theta-comparison.csv"
CSV_SIMILARITY_COMP = "./csv/similarity-comparison.csv"

FIGURE_6 = "figure6.pdf"
FIGURE_7 = "figure7.pdf"
FIGURE_8 = "figure8.pdf"
FIGURE_9 = "figure9.pdf"

AGG_FUNCS = {
    "similarity": "mean",
    "time": "mean",
    "avg_transfo_length": "mean",
    "total_transfo": "max",
}


# Helper Functions


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
    res = sum(x / y for x, y in zip(e1, e2)) / len(e1)
    return res


def total_transfo(s):
    if not isinstance(s, str):
        return np.nan
    s = s.strip("'")
    if len(s) == 0:
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
    if len(s) == 0:
        return np.nan
    total = 0
    transfos = s.split(":")
    for t in transfos:
        for v in t.split(";"):
            if len(v) > 0:
                total += 1
    return total / len(transfos)


# Data Loading


def load_one_pruning(filename):
    pruning = pd.read_csv(filename)
    new_index = pruning["index"].str.extract(r"([^-]*)-(.+)-(\d+)")
    new_index[2] = new_index[2].astype(int)
    pruning.index = pd.MultiIndex.from_frame(new_index)
    pruning = pruning.drop(columns="index")
    return pruning


def load_pruning_dataset():
    try:
        sims = pd.read_csv(CSV_SIMILARITIES)
        sims = sims[sims["inserted"] == 0]
        sims.index = pd.Index(sims["dataset"])
        sims.index.names = ["dataset"]
        sims = sims.drop(columns=["inserted", "dataset"])
    except FileNotFoundError:
        print(
            f"Warning: {CSV_SIMILARITIES} not found. Creating mock data for testing."
        )
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
            pruning[col] = pruning[col].astype(str).str.findall(r"(\d+(?:\.\d+)?|None)")

    pruning["transfo_path"] = pruning["transfo_path"].fillna("['', '', '', '', '']")
    pruning["transfo_path"] = pruning["transfo_path"].str.findall(r"'.*?'")

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

    return pruning


def load_increasing_dataset():
    sims = pd.read_csv(CSV_SIMILARITIES)
    sims.index = pd.MultiIndex.from_frame(sims[["dataset", "inserted"]])
    sims = sims.drop(columns=["dataset", "inserted"])

    increasing = pd.read_csv(CSV_SCHEMA_SIZE)
    new_index = increasing["index"].str.extract(r"([^-]*)-(.+)-sources-(\d+).*")
    new_index.columns = ["dataset", "strat", "inserted"]
    new_index["inserted"] = new_index["inserted"].astype(int)
    increasing.index = pd.MultiIndex.from_frame(new_index)
    increasing = increasing.drop(columns="index")
    cols = [
        "similarity",
        "path",
        "time",
    ]
    increasing = increasing.drop(
        columns=["souffle_time", "neo4j_time", "sim_time", "gen_time", "automaton_time"]
    )
    for col in cols:
        increasing[col] = increasing[col].str.findall(r"(\d+(?:\.\d+)?|None)")
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
    print(increasing)
    mask = increasing.isna().any(axis=1).groupby(["dataset", "strat"]).transform("any")
    increasing = increasing[~mask]
    return increasing


def load_additional_data():
    similarities = pd.read_csv(CSV_SIMILARITIES)
    similarities = similarities[similarities["inserted"] == 0].drop("inserted", axis=1)
    similarities.index = pd.Index(similarities["dataset"])
    similarities.drop("dataset", axis=1, inplace=True)

    dataset_map = {
        "persondata": "D1",
        "dblp_to_amalgam1": "D2",
        "amalgam1_to_amalgam3": "D3",
        "flighthotel": "D4",
    }
    display_order = ["D1", "D2", "D3", "D4"]
    dataset_order = [
        "persondata",
        "dblp_to_amalgam1",
        "amalgam1_to_amalgam3",
        "flighthotel",
    ]
    strat_order = ["greedy", "naive", "random", "weighted_distance"]

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
    print(path_data)
    data = data.join(path_data, on="dataset")
    data["path"] = data["path"] / data["best_path"]
    return data, theta_data, minhash_data


# Grid Layout


def setup_grid_layout(outer_items, inner_items, inner_shape=(2, 2)):
    n_outer = len(outer_items)
    rows, cols = inner_shape

    fig_width = 4 * cols * n_outer
    fig_height = params.HEIGHT_FACTOR * rows
    fig = plt.figure(figsize=(fig_width, fig_height))

    outer_grid = gridspec.GridSpec(1, n_outer, figure=fig, wspace=0.1)

    axes_structure = []

    for i in range(n_outer):
        inner_grid = outer_grid[i].subgridspec(
            rows, cols, wspace=0.2, hspace=params.HSPACE
        )

        block_axes = []
        for r in range(rows):
            for c in range(cols):
                idx = r * cols + c
                if idx < len(inner_items):
                    ax = fig.add_subplot(inner_grid[r, c])
                    block_axes.append(ax)
                else:
                    block_axes.append(None)
        axes_structure.append(block_axes)

    return fig, axes_structure


def get_aggregated_data(df, metrics, x):
    current_agg = {m: AGG_FUNCS.get(m, "mean") for m in metrics}
    df_agg = df.groupby(["dataset", "strat", x])[metrics].agg(current_agg).reset_index()
    return df_agg


def get_aggregated_data_pruning(df, metrics):
    current_agg = {m: AGG_FUNCS.get(m, "mean") for m in metrics}
    df_agg = (
        df.groupby(["dataset", "strat", "pruning"])[metrics]
        .agg(current_agg)
        .reset_index()
    )
    return df_agg


# Plot: Pruning


def make_figure_6(df):
    datasets = ["persondata", "dblp_to_amalgam1", "amalgam1_to_amalgam3", "flighthotel"]
    if len(datasets) > 4:
        print(
            f"Warning: Found {len(datasets)} datasets, but 2x2 grid fits 4. Truncating."
        )
        datasets = datasets[:4]

    metrics_to_plot = ["similarity", "time", "total_transfo"]

    outer_blocks = metrics_to_plot
    inner_items = datasets

    df_agg = get_aggregated_data_pruning(df, metrics_to_plot)

    fig, axes_matrix = setup_grid_layout(outer_blocks, inner_items, inner_shape=(2, 2))

    legend_handles = []
    legend_labels = []

    for block_idx, metric in enumerate(outer_blocks):
        block_axes = axes_matrix[block_idx]
        metric_name = METRIC_LABELS_PRUNING.get(metric, metric)

        if block_axes[0]:
            block_axes[0].annotate(
                params.label_block(block_idx, metric_name),
                xy=(1.1, 1.2),
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

            sns_ax = sb.pointplot(
                data=subset,
                x="pruning",
                y=metric,
                hue="strat",
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette="tab10",
                ax=ax,
                scale=0.8,
            )

            ax.set_title(DATASET_LABELS.get(dataset, dataset), fontsize=11)
            ax.set_xlabel("")
            ax.set_ylabel("")

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
    plt.subplots_adjust(top=params.TOP_LEGEND)

    output_file = os.path.join(OUTPUT_DIR, FIGURE_6)
    plt.savefig(output_file, bbox_inches="tight")
    print(f"Plot saved to {output_file}")


# Plot: Additional


def make_figure_9(data, theta_data, minhash_data):
    datasets = ["persondata", "dblp_to_amalgam1", "amalgam1_to_amalgam3", "flighthotel"]

    metrics_to_plot = ["similarity", "time", "path", "dup"]

    outer_blocks = [
        (data, "dataset", "dataset", "dataset"),
        (theta_data, "similarity", "theta", METRIC_LABELS_ADDITIONAL["theta"]),
        (minhash_data, "similarity", "minhash", METRIC_LABELS_ADDITIONAL["minhash"]),
    ]
    inner_items = datasets

    fig, axes_matrix = setup_grid_layout(outer_blocks, inner_items, inner_shape=(2, 2))

    legend_handles = []
    strats = ["greedy", "naive", "random", "weighted_distance"]
    colors = sb.color_palette("tab10", n_colors=len(strats))
    color_dict = dict(zip(strats, colors))
    legend_labels = []

    for block_idx, (df, metric, x, x_label) in enumerate(outer_blocks):
        df_agg = df.reset_index()
        block_axes = axes_matrix[block_idx]
        metric_name = METRIC_LABELS_ADDITIONAL.get(metric, metric)
        if block_idx == 0:
            dataset_map = {
                "persondata": "D1",
                "dblp_to_amalgam1": "D2",
                "amalgam1_to_amalgam3": "D3",
                "flighthotel": "D4",
            }
            df_agg["dataset"] = df_agg["dataset"].map(dataset_map)
            block_axes[0].annotate(
                params.label_block(block_idx, "Datasets"),
                xy=(1.1, 1.2),
                xycoords="axes fraction",
                ha="center",
                fontsize=14,
                fontweight="bold",
            )
            for index, metric in enumerate(["similarity", "time", "path", "dup"]):
                ax = block_axes[index]
                sns_ax = sb.pointplot(
                    data=df_agg,
                    x=x,
                    y=metric,
                    hue="strategy",
                    dodge=0.2,
                    markers="o",
                    linestyles="-",
                    errorbar=None,
                    palette=color_dict,
                    ax=ax,
                    scale=0.8,
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
                params.label_block(block_idx, metric_name),
                xy=(1.1, 1.2),
                xycoords="axes fraction",
                ha="center",
                fontsize=14,
                fontweight="bold",
            )
        if block_idx == 2:
            df_agg["minhash"] = df_agg["minhash"].map(
                {"None": "Jaccard", "10": "10", "50": "50", "100": "100", "200": "200"}
            )
        if block_idx == 1:
            df_agg["theta"] = df_agg["theta"].map(
                {x: str(round(1 - float(x), 2)) for x in ["0.5", "0.7", "0.9", "1.0"]}
            )

        for item_idx, dataset in enumerate(inner_items):
            ax = block_axes[item_idx]
            if ax is None:
                continue

            subset = df_agg[df_agg["dataset"] == dataset]

            sns_ax = sb.pointplot(
                data=subset,
                x=x,
                y=metric,
                hue="strategy",
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette=color_dict,
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
    plt.subplots_adjust(top=params.TOP_LEGEND)

    output_file = os.path.join(OUTPUT_DIR, FIGURE_8)
    plt.savefig(output_file, bbox_inches="tight")
    print(f"Plot saved to {output_file}")


# Plot: Increasing


def make_figure_7(prun_df, inc_df):
    datasets = ["persondata", "dblp_to_amalgam1", "amalgam1_to_amalgam3", "flighthotel"]
    if len(datasets) > 4:
        print(
            f"Warning: Found {len(datasets)} datasets, but 2x2 grid fits 4. Truncating."
        )
        datasets = datasets[:4]

    metrics_to_plot = ["similarity", "time", "total_transfo"]

    outer_blocks = [
        (prun_df, "avg_transfo_length", "pruning", "# cand. schema"),
        (inc_df, "similarity", "inserted", "# added nodes"),
        (inc_df, "time", "inserted", "# added nodes"),
    ]
    inner_items = datasets

    fig, axes_matrix = setup_grid_layout(outer_blocks, inner_items, inner_shape=(2, 2))

    legend_handles = []
    legend_labels = []
    strats = ["greedy", "naive", "random", "weighted_distance"]
    colors = sb.color_palette("tab10", n_colors=len(strats))
    color_dict = dict(zip(strats, colors))

    for block_idx, (df, metric, x, x_label) in enumerate(outer_blocks):
        df_agg = get_aggregated_data(df, [metric], x)
        block_axes = axes_matrix[block_idx]
        metric_name = METRIC_LABELS_INCREASING.get(metric, metric)

        if block_axes[0]:
            block_axes[0].annotate(
                params.label_block(block_idx, metric_name),
                xy=(1.1, 1.2),
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

            sns_ax = sb.pointplot(
                data=subset,
                x=x,
                y=metric,
                hue="strat",
                dodge=0.4,
                markers="o",
                linestyles="-",
                errorbar=None,
                palette=color_dict,
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
    plt.subplots_adjust(top=params.TOP_LEGEND)

    output_file = os.path.join(OUTPUT_DIR, FIGURE_7)
    plt.savefig(output_file, bbox_inches="tight")
    print(f"Plot saved to {output_file}")


# Plot: Stacked Bars


def make_stackbars_plot():
    cols = ["souffle_time", "neo4j_time", "sim_time", "gen_time", "automaton_time"]
    col_names = {
        "souffle_time": "Edit. op. gen.",
        "neo4j_time": "Metagraph const.",
        "sim_time": "Sim. comp.",
        "gen_time": "Schema gen.",
        "automaton_time": "Transfo. gen.",
    }
    datasets = ["persondata", "dblp_to_amalgam1", "amalgam1_to_amalgam3", "flighthotel"]
    dataset_names = {ds: f"D{i + 1}" for i, ds in enumerate(datasets)}
    colors = sb.color_palette("tab10", n_colors=len(cols))
    # hatches = [None, "///", "...", "\\\\\\", "***"]

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
    print(data)

    for i, col in enumerate(cols):
        values = data[col].to_numpy()
        ax.barh(
            x_labels,
            values,
            left=bottom,
            # hatch=hatches[i],
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

    filename = os.path.join(OUTPUT_DIR, FIGURE_9)
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    print(f"Plot saved to {filename}")


# Main Execution

if __name__ == "__main__":
    try:
        if not os.path.isdir(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

        print("Loading pruning data...")
        pruning_data = load_pruning_dataset()

        if not pruning_data.empty:
            print("Generating pruning plot...")
            make_figure_6(pruning_data)
        else:
            print("No pruning data loaded.")

        print("Loading increasing data...")
        increasing_data = load_increasing_dataset()

        if not pruning_data.empty:
            print("Generating increasing plot...")
            make_figure_7(pruning_data, increasing_data)
        else:
            print("No data loaded for increasing plot.")

        print("Loading additional data...")
        additional_data = load_additional_data()
        print("Generating additional plot...")
        make_figure_9(*additional_data)

        print("Generating stacked bars plot...")
        make_stackbars_plot()

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback

        traceback.print_exc()
