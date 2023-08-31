import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import sys
import argparse
from pathlib import Path

sys.path.append("/foss_fim/tools/")
from tools_shared_variables import BAD_SITES

from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import mpl_toolkits.axisartist as AA

mag_dict = {
    "action": "1_act",
    "minor": "2_min",
    "moderate": "3_mod",
    "major": "4_maj",
    "100yr": 100,
    "500yr": 500,
    "2yr": 2,
    "5yr": 5,
    "10yr": 10,
    "25yr": 25,
    "50yr": 50,
    "200yr": 200,
}
inverted_mag = dict((v, k) for k, v in mag_dict.items())


def eval_plot_stack_data_prep(metric_csv, versions=[]):
    # Load in FIM 4 CSV and select the proper version metrics
    metrics = pd.read_csv(metric_csv, dtype={"huc": str})
    if versions:
        versions = list(versions)
        # Check to make sure requested versions are in the metrics file
        for v in versions:
            assert (
                v in metrics.version.unique()
            ), f"{v} is not included in {metric_csv}.\nThe available options are {sorted(metrics.version.unique())}"
        # Filter the metrics to the requested versions
        metrics = metrics.loc[metrics.version.isin(versions)]
    # Change the magnitudes to make them more sort-friendly for the plot below
    metrics.magnitude = metrics.magnitude.apply(lambda m: mag_dict[m])
    # Fill the non-AHPS sites with values for nws_lid
    metrics.nws_lid.fillna(metrics.huc + "_" + metrics.benchmark_source, inplace=True)
    # Create multi-index for easy merging and plotting
    metrics.set_index(
        ["benchmark_source", "nws_lid", "magnitude"], inplace=True, drop=False
    )

    # Normalize data to the total of TP + FN
    metrics["TP_FN_sum"] = (
        metrics["false_negatives_count"] + metrics["true_positives_count"]
    )
    metrics["TP_norm"] = metrics["true_positives_count"] / metrics["TP_FN_sum"]
    metrics["FN_norm"] = metrics["false_negatives_count"] / metrics["TP_FN_sum"]
    metrics["FP_norm"] = metrics["false_positives_count"] / metrics["TP_FN_sum"]
    metrics["FP_norm"].replace(
        [np.inf, -np.inf], np.nan, inplace=True
    )  # some bad sites will divide by 0

    return metrics


def eval_plot_stack_indiv(metric_csv, versions, outfig, category):
    # Check inputs
    assert category in (
        "nws",
        "usgs",
        "ble",
        "ifc",
        "ras2fim",
    ), f"category must be one of ('nws','usgs','ble','ifc','ras2fim'), not {category}"

    metrics_df = eval_plot_stack_data_prep(metric_csv, versions)

    # Filter the plotting data to the selected category
    data = metrics_df.loc[(category)]
    num_subplots = len(data.nws_lid.unique())
    xmax = data["FP_norm"].max() + 1
    # Save version input order for plotting
    version_dict = {v: i for i, v in enumerate(versions)}
    data = data.assign(plot_order=data["version"])
    data["plot_order"] = data.plot_order.map(version_dict)
    data["magnitude_"] = data.magnitude
    # Create the plot
    fig, ax = plt.subplots(
        num_subplots,
        1,
        figsize=(8, len(data) * 0.25),
        dpi=100,
        facecolor="white",
        subplot_kw={"axes_class": AA.Axes},
    )
    # Create a subplot for every site (nws_lid)
    for i, site in enumerate(sorted(data.nws_lid.unique())):
        subplot_data = data.loc[(site)]
        subplot_data = subplot_data.sort_values(
            ["magnitude_", "plot_order"], ascending=False
        )
        new_y = [j * 1.25 for j in range(len(subplot_data))]
        # Stacked horizontal bar plots
        ax[i].barh(
            y=new_y,
            width="TP_norm",
            left=0.0,
            color="#2c7bb6",
            linewidth=0.5,
            data=subplot_data,
        )
        ax[i].barh(
            y=new_y,
            width="FN_norm",
            left="TP_norm",
            color="#fdae61",
            linewidth=0.5,
            data=subplot_data,
        )
        ax[i].barh(
            y=new_y,
            width="FP_norm",
            left=1.0,
            color="#d7191c",
            linewidth=0.5,
            data=subplot_data,
        )
        # Plot the CSI and MCC scores
        ax[i].scatter(
            y=new_y,
            x=subplot_data["CSI"].array,
            c="k",
            s=15,
            marker=r"x",
            zorder=3,
            linewidth=0.75,
        )
        ax[i].scatter(
            y=new_y,
            x=subplot_data["MCC"].array,
            s=15,
            marker=r"o",
            zorder=3,
            linewidths=0.75,
            facecolor="None",
            edgecolors="k",
        )
        # Various axis customizations
        ax[i].set_yticks(
            new_y,
            labels=subplot_data.version
            + " "
            + subplot_data.magnitude.map(inverted_mag).astype(str),
        )
        ax[i].axis["left"].label.set(
            visible=True, text=site, rotation=90, pad=10, ha="right"
        )
        ax[i].axis["left"].major_ticks.set(tick_out=True)
        ax[i].axis["left"].major_ticklabels.set(ha="left")
        ax[i].set_xlim(0, xmax)
        ax[i].set_ylim(-1, new_y[-1] + 1)
        num_mags = len(subplot_data.magnitude.unique())
        hlines = [
            j
            for j in np.linspace(
                new_y[-1] / num_mags, new_y[-1], num_mags - 1, endpoint=False
            )
        ]
        ax[i].hlines(
            hlines,
            xmin=0,
            xmax=xmax,
            colors=["0.9" if i % 2 == 0 else "w"] * (num_mags - 1),
            linestyles="dotted",
            zorder=0,
        )
        ax[i].grid(axis="x", color="0.8", zorder=0)
        ax[i].axis["bottom"].set_visible(True if i == num_subplots - 1 else False)
        ax[i].axis["top"].set_visible(True if i == 0 else False)
        ax[i].axis["top"].major_ticklabels.set_visible(True if i == 0 else False)
        ax[i].set_facecolor("w" if i % 2 == 0 else "0.9")
        # Label sites that have been identified as "Bad"
        if site and site in BAD_SITES:
            ax[i].text(
                xmax / 2,
                new_y[-1] / 2,
                "--BAD SITE--",
                horizontalalignment="center",
                verticalalignment="center",
            )
            ax[i].set_facecolor("0.67")
    plt.subplots_adjust(wspace=0, hspace=0)
    ax[0].set_title(
        f"{category.upper()} FIM Evaluation | Individual Sites", loc="center", pad=40
    )
    TP_patch = Patch(color="#2c7bb6", linewidth=0.5, label=f"True Positive")
    FN_patch = Patch(color="#fdae61", linewidth=0.5, label=f"False Negative")
    FP_patch = Patch(color="#d7191c", linewidth=0.5, label=f"False Positive")
    x_marker = Line2D(
        [0],
        [0],
        marker="x",
        color="None",
        markeredgecolor="k",
        markerfacecolor="k",
        label="CSI Score",
        markersize=5,
        linewidth=0.5,
    )
    o_marker = Line2D(
        [0],
        [0],
        marker="o",
        color="None",
        markeredgecolor="k",
        markerfacecolor="None",
        label="MCC Score",
        markersize=5,
        linewidth=10,
    )
    # Get the height of the figure in pixels so we can put the legend in a consistent position
    ax_pixel_height = (
        ax[0].get_window_extent().transformed(fig.dpi_scale_trans.inverted()).height
    )
    handles = [TP_patch, FN_patch, FP_patch, x_marker, o_marker]
    ax[0].legend(
        loc="center",
        ncol=len(handles),
        handles=handles,
        fontsize=8,
        columnspacing=1,
        bbox_to_anchor=(0.5, (0.3 + ax_pixel_height) / ax_pixel_height),
    )
    plt.savefig(outfig, bbox_inches="tight")

    return metrics_df


def eval_plot_stack(metric_csv, versions, category, outfig, show_iqr=False):
    # Check inputs
    assert category in (
        "nws",
        "usgs",
        "ble",
        "ifc",
        "ras2fim",
    ), f"category must be one of ('nws','usgs','ble','ifc','ras2fim'), not {category}"

    metrics_df = eval_plot_stack_data_prep(metric_csv, versions)
    metrics_df = metrics_df.loc[~metrics_df.nws_lid.isin(BAD_SITES)]
    grouped = metrics_df.reset_index(drop=True).groupby(
        ["benchmark_source", "version", "magnitude"], sort=False
    )
    count_df = grouped.count()["CSI"]
    metrics_df = grouped.median(numeric_only=True)
    metrics_df["TP_norm_q1"] = metrics_df["TP_norm"] - grouped["TP_norm"].quantile(0.25)
    metrics_df["TP_norm_q3"] = grouped["TP_norm"].quantile(0.75) - metrics_df["TP_norm"]
    metrics_df["FP_norm_q1"] = metrics_df["FP_norm"] - grouped["FP_norm"].quantile(0.25)
    metrics_df["FP_norm_q3"] = grouped["FP_norm"].quantile(0.75) - metrics_df["FP_norm"]

    # Filter the plotting data to the selected category
    data = metrics_df.loc[(category)].swaplevel()
    num_subplots = len(data.reset_index().magnitude.unique())
    num_ver = len(data.reset_index().version.unique())
    xmax = 1 + data["FP_norm"].max() + data["FP_norm_q3"][data["FP_norm"].idxmax()]
    # Save version input order for plotting
    version_dict = {v: i for i, v in enumerate(versions)}
    data = data.assign(plot_order=data.index.get_level_values("version"))
    data["plot_order"] = data.plot_order.map(version_dict)

    # Create the plot
    fig, ax = plt.subplots(
        num_subplots,
        1,
        figsize=(8, len(data) * 0.25),
        dpi=100,
        facecolor="white",
        subplot_kw={"axes_class": AA.Axes},
    )
    # Create a subplot for every flow (nws_lid)
    for i, mag in enumerate(sorted(data.reset_index().magnitude.unique())):
        subplot_data = data.loc[(mag)]
        subplot_data = subplot_data.sort_values(["plot_order"], ascending=False)
        new_y = [j * 1.25 for j in range(len(subplot_data))]
        # Stacked horizontal bar plots
        ax[i].barh(
            y=new_y,
            width="TP_norm",
            left=0.0,
            color="#2c7bb6",
            linewidth=0.5,
            data=subplot_data,
            xerr=[subplot_data["TP_norm_q1"], subplot_data["TP_norm_q3"]]
            if show_iqr
            else None,
            error_kw=dict(elinewidth=1),
        )
        ax[i].barh(
            y=new_y,
            width="FN_norm",
            left="TP_norm",
            color="#fdae61",
            linewidth=0.5,
            data=subplot_data,
        )
        ax[i].barh(
            y=new_y,
            width="FP_norm",
            left=1.0,
            color="#d7191c",
            linewidth=0.5,
            data=subplot_data,
            xerr=[subplot_data["FP_norm_q1"], subplot_data["FP_norm_q3"]]
            if show_iqr
            else None,
            error_kw=dict(elinewidth=1),
        )
        # Plot the CSI and MCC scores
        ax[i].scatter(
            y=new_y,
            x=subplot_data["CSI"].array,
            c="k",
            s=15,
            marker=r"x",
            zorder=3,
            linewidth=0.75,
        )
        ax[i].scatter(
            y=new_y,
            x=subplot_data["MCC"].array,
            s=15,
            marker=r"o",
            zorder=3,
            linewidths=0.75,
            facecolor="None",
            edgecolors="k",
        )
        # Various axis customizations
        ax[i].set_yticks(new_y, labels=subplot_data.index)  # , ha='left')
        ax[i].axis["left"].label.set(
            visible=True, text=inverted_mag[mag], rotation=90, pad=10, ha="right"
        )
        ax[i].axis["left"].major_ticks.set(tick_out=True)
        ax[i].axis["left"].major_ticklabels.set(ha="left")
        n = count_df.loc[(category, versions[0], mag)]
        ax[i].axis["right"].label.set(
            visible=True, text=f"n={n}", rotation=270, pad=5, ha="left"
        )
        ax[i].set_xlim(0, xmax)
        ax[i].set_ylim(-1.25, num_ver + 1.25)
        ax[i].grid(axis="x", color="0.8", zorder=0)
        ax[i].axis["bottom"].set_visible(True if i == num_subplots - 1 else False)
        ax[i].axis["top"].set_visible(True if i == 0 else False)
        ax[i].axis["top"].major_ticklabels.set_visible(True if i == 0 else False)
        ax[i].set_facecolor("w" if i % 2 == 0 else "0.9")
    plt.subplots_adjust(wspace=0, hspace=0)
    ax[0].set_title(f"{category.upper()} FIM Evaluation", loc="center", pad=35)
    TP_patch = Patch(color="#2c7bb6", linewidth=0.5, label=f"True Positive")
    FN_patch = Patch(color="#fdae61", linewidth=0.5, label=f"False Negative")
    FP_patch = Patch(color="#d7191c", linewidth=0.5, label=f"False Positive")
    x_marker = Line2D(
        [0],
        [0],
        marker="x",
        color="None",
        markeredgecolor="k",
        markerfacecolor="k",
        label="CSI Score",
        markersize=5,
        linewidth=10,
    )
    o_marker = Line2D(
        [0],
        [0],
        marker="o",
        color="None",
        markeredgecolor="k",
        markerfacecolor="None",
        label="MCC Score",
        markersize=5,
        linewidth=10,
    )
    # Get the height of the figure in pixels so we can put the legend in a consistent position
    ax_pixel_height = (
        ax[0].get_window_extent().transformed(fig.dpi_scale_trans.inverted()).height
    )
    handles = [TP_patch, FN_patch, FP_patch, x_marker, o_marker]
    ax[0].legend(
        loc="center",
        ncol=len(handles),
        handles=handles,
        fontsize=8,
        columnspacing=1,
        bbox_to_anchor=(0.5, (0.3 + ax_pixel_height) / ax_pixel_height),
    )
    plt.savefig(outfig, bbox_inches="tight")

    return data


def diff_bar_plots(versions, metric_csv, category, outfig, stat="CSI"):
    # Check inputs
    assert category in (
        "nws",
        "usgs",
        "ble",
        "ifc",
        "ras2fim",
    ), f"category must be one of ('nws','usgs','ble','ifc','ras2fim'), not {category}"
    assert stat in (
        "CSI",
        "MCC",
        "TPR",
        "PND",
        "FAR",
    ), f"stat must be one of ('CSI', 'MCC', 'TPR', 'PND', 'FAR'), not {stat}"
    assert (
        len(versions) == 2
    ), f"You can only produce the comparison plots when comparing 2 fim versions. You have input {len(versions)}."
    # CSI, MCC, and TPR scores are better when closer to 1, however PND and FAR scores are better when closer to 0
    better_score = "positive" if stat in ("CSI", "MCC", "TPR") else "negative"

    # Load in FIM 4 CSV and select the proper version metrics
    metrics = pd.read_csv(metric_csv, dtype={"huc": str})
    # Check to make sure requested versions are in the metrics file
    for v in versions:
        assert (
            v in metrics.version.unique()
        ), f"{v} is not included in {metric_csv}.\nThe available options are {sorted(metrics.version.unique())}"
    # Change the magnitudes to make them more sort-friendly for the plot below
    metrics.magnitude = metrics.magnitude.apply(lambda m: mag_dict[m])
    # Fill the non-AHPS sites with values for nws_lid
    metrics.nws_lid.fillna(metrics.huc + "_" + metrics.benchmark_source, inplace=True)
    # Create multi-index for easy merging and plotting
    metrics.set_index(
        ["benchmark_source", "nws_lid", "magnitude"], inplace=True, drop=False
    )

    # Separate versions into 2 dataframes
    metrics_base = metrics.loc[metrics.version == versions[0]].copy()
    metrics_new = metrics.loc[metrics.version == versions[1]].copy()

    # Create a new column in the base dataframe and merge it into the comparison dataframe
    metrics_base[f"BASE_{stat}"] = metrics_base[stat]
    metrics_base.drop(columns=stat, inplace=True)
    fim_compare = metrics_base.merge(
        metrics_new[stat], left_index=True, right_index=True, how="inner"
    ).sort_index(level=0, sort_remaining=True)
    fim_compare[f"{stat}_diff"] = fim_compare[stat] - fim_compare[f"BASE_{stat}"]
    # Color the boxes according to improved vs regressed scores. See definition of 'better_score' above for details.
    if better_score == "positive":
        fim_compare["color"] = fim_compare.apply(
            lambda row: "None" if row[f"{stat}_diff"] < 0 else "g", axis=1
        )
        fim_compare["edge_color"] = fim_compare.apply(
            lambda row: "r" if row[f"{stat}_diff"] < 0 else "g", axis=1
        )
    elif better_score == "negative":
        fim_compare["color"] = fim_compare.apply(
            lambda row: "None" if row[f"{stat}_diff"] > 0 else "g", axis=1
        )
        fim_compare["edge_color"] = fim_compare.apply(
            lambda row: "r" if row[f"{stat}_diff"] > 0 else "g", axis=1
        )

    # Filter the plotting data to the selected category
    data = fim_compare.loc[(category)]
    num_subplots = len(data.nws_lid.unique())
    num_mags = len(data.magnitude.unique())
    # Create the plot
    fig, ax = plt.subplots(
        num_subplots, 1, figsize=(8, len(data) * 0.18), dpi=100, facecolor="white"
    )
    # Create a subplot for every site (nws_lid)
    for i, site in enumerate(data.nws_lid.unique()):
        subplot_data = data.loc[(site)]
        ax[i].barh(
            y=np.arange(len(subplot_data)),
            width=f"{stat}_diff",
            left=f"BASE_{stat}",
            color="color",
            edgecolor="edge_color",
            linewidth=0.5,
            data=subplot_data,
            zorder=3,
        )
        ax[i].set_yticks(
            np.arange(len(subplot_data)),
            labels=subplot_data.index.map(inverted_mag).astype(str),
        )
        ax[i].set_xticks(np.linspace(0, 1, 11))
        ax[i].set_ylabel(site, rotation="horizontal", labelpad=40)
        ax[i].set_xlim(0, 1)
        ax[i].set_ylim(-0.5, num_mags - 0.25)
        ax[i].grid(axis="x", color="0.8", zorder=0)
        ax[i].spines["bottom"].set_color("0" if i == num_subplots - 1 else "None")
        ax[i].spines["top"].set_color("0" if i == 0 else "0.8")
        ax[i].set_facecolor("w" if i % 2 == 0 else "0.9")
        ax[i].tick_params(
            axis="x",
            bottom=True if i == num_subplots - 1 else False,
            top=True if i == 0 else False,
            labelbottom=True if i == num_subplots - 1 else False,
            labeltop=True if i == 0 else False,
        )
        # Label sites that have been identified as "Bad"
        if site and site in BAD_SITES:
            ax[i].text(
                0.1,
                1.5,
                "--BAD SITE--",
                horizontalalignment="center",
                verticalalignment="center",
            )
            ax[i].set_facecolor("0.67")
    plt.subplots_adjust(wspace=0, hspace=0)
    ax[0].set_title(
        f"{category.upper()} {stat} Comparison\n{versions[0]}  v  {versions[1]}",
        loc="center",
        pad=45,
    )
    g_patch = Patch(color="g", linewidth=0.5, label=f"{stat} Score Improvement")
    r_patch = Patch(
        facecolor="None", edgecolor="r", linewidth=0.5, label=f"{stat} Score Regression"
    )
    # Get the height of the figure in pixels so we can put the legend in a consistent position
    ax_pixel_height = (
        ax[0].get_window_extent().transformed(fig.dpi_scale_trans.inverted()).height
    )
    ax[0].legend(
        loc="center",
        ncol=2,
        handles=[g_patch, r_patch],
        fontsize=8,
        bbox_to_anchor=(0.5, (0.4 + ax_pixel_height) / ax_pixel_height),
    )
    plt.savefig(outfig, bbox_inches="tight")

    return fim_compare


def iter_benchmarks(
    metric_csv,
    workspace,
    versions=[],
    individual_plots=False,
    show_iqr=False,
    diff_stat=False,
):
    # Import metrics csv as DataFrame and initialize all_datasets dictionary
    csv_df = pd.read_csv(metric_csv, dtype={"huc": str})

    # If versions are supplied then filter out
    if versions:
        # Filter out versions based on supplied version list
        metrics = csv_df.query("version == @versions")
    else:
        metrics = csv_df

    # Group by benchmark source
    benchmark_by_source = metrics.groupby(["benchmark_source", "extent_config"])
    for (
        benchmark_source,
        extent_configuration,
    ), benchmark_metrics in benchmark_by_source:
        # Define and create the output workspace as a subfolder within the supplied workspace
        output_workspace = (
            Path(workspace) / benchmark_source / extent_configuration.lower()
        )
        output_workspace.mkdir(parents=True, exist_ok=True)
        output_png = (
            Path(output_workspace)
            / f"{benchmark_source}_{extent_configuration.lower()}_stackedbar{'_indiv'if individual_plots else ''}.png"
        )
        if diff_stat:
            output_png = (
                Path(output_workspace)
                / f"{benchmark_source}_{extent_configuration.lower()}_diff_plot_{diff_stat}.png"
            )
            diff_bar_plots(
                versions,
                metric_csv,
                category=benchmark_source,
                outfig=output_png,
                stat=diff_stat,
            )
        elif individual_plots:
            output_png = (
                Path(output_workspace)
                / f"{benchmark_source}_{extent_configuration.lower()}_stackedbar_indiv.png"
            )
            eval_plot_stack_indiv(
                metric_csv=metric_csv,
                versions=versions,
                category=benchmark_source,
                outfig=output_png,
            )
        else:
            output_png = (
                Path(output_workspace)
                / f"{benchmark_source}_{extent_configuration.lower()}_stackedbar.png"
            )
            eval_plot_stack(
                metric_csv=metric_csv,
                versions=versions,
                category=benchmark_source,
                show_iqr=show_iqr,
                outfig=output_png,
            )


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description=f"Plot and aggregate statistics for benchmark datasets (BLE/AHPS libraries)"
    )
    parser.add_argument(
        "-m",
        "--metric_csv",
        help="Metrics csv created from synthesize test cases.",
        required=True,
    )
    parser.add_argument("-w", "--workspace", help="Output workspace", required=True)
    parser.add_argument(
        "-v",
        "--versions",
        help='List of versions to be plotted/aggregated. Versions are filtered using the "startswith" approach. For example, ["fim_","fb1"] would retain all versions that began with "fim_" (e.g. fim_1..., fim_2..., fim_3...) as well as any feature branch that began with "fb". An other example ["fim_3","fb"] would result in all fim_3 versions being plotted along with the fb.',
        nargs="+",
        default=[],
    )
    parser.add_argument(
        "-i",
        "--site-plots",
        help="If enabled individual barplots for each site are created.",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-iqr",
        "--show-iqr",
        help="If enabled, inter-quartile range error bars will be added.",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-d",
        "--diff-stat",
        help="This option creates diff plots instead of stacked bar plots. Only 2 versions can be used with this option. "
        + "Input one of the following statistics to be used for comparison: ('CSI', 'MCC', 'TPR', 'PND', 'FAR').",
        default=False,
        required=False,
    )

    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())

    # Finalize Variables
    m = args["metric_csv"]
    w = args["workspace"]
    v = args["versions"]
    i = args["site_plots"]
    iqr = args["show_iqr"]
    d = args["diff_stat"]

    # Run eval_plots function
    print(
        'The following AHPS sites are considered "BAD_SITES":  ' + ", ".join(BAD_SITES)
    )
    iter_benchmarks(m, w, v, i, iqr, d)
