from __future__ import annotations

import math
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, LogLocator, FuncFormatter

# Constants
HASH_SIZE_BYTES = 32     # one SHA-256 hash, common to every approach
SHORT_SIZE_BYTES = 2 + 1 # a "short" = 16-bit reference index
BASELINE_BYTES_PER_REF = 40

APPROACH_COLORS = {
    "sota": "#8C8C8C",
    "bitset": "#4C72B0",
    "missing_ids": "#55A868",
    "prev_delta": "#C44E52",
    "compressed_bitset": "#8172B2",
}
APPROACH_LABELS = {
    "sota": "SOTA (full reference list)",
    "bitset": "Fixed Bitset",
    "missing_ids": "Missing Identities (Short)",
    "prev_delta": "Previous Round Delta (Short)",
    "compressed_bitset": "Compressed Bitset (\u00bd)",
}

# block_size (KB):       mean=4.113  p1=3.332 p10=3.723 p50=4.074  p90=4.445  p99=5.598
# num_transactions:      mean=0.333 p1=0.000 p10=0.000 p50=0.000  p90=1.000  p99=2.000
# actual num_tx:         mean=0.333 p1=0.000 p10=0.000 p50=0.000  p90=1.000  p99=2.000
# tx_pl_size_per_b (KB): mean=144.002
# tx_size:               mean=399.900  p1=298.000 p10=298.000 p50=306.000  p90=580.000  p99=2023.000
# num_references:        mean=98.363  p1=81.000 p10=91.000 p50=99.000  p90=104.000  p99=106.000 max=108.000
# references_size (KB):  mean=3.842  p1=3.164 p10=3.555 p50=3.867  p90=4.062  p99=4.141
# % of total:            mean=0.93971 p1=0.692 p10=0.898 p50=0.966  p90=0.969  p99=0.970
# overlap:               mean=13.877 p1=1.000 p10=6.000 p50=13.000  p90=23.000  p99=33.000 max=71.000
# bitmap-size:           mean=9.047 p1=8.000 p10=8.000 p50=8.000  p90=18.000  p99=18.000 max=18.000
# 1205000 rounds
data_set_1 = {
    "max_participants": 108,
    "num_references": {
        "p1": 81,
        "p10": 91,
        "p50": 99,
        "p90": 104,
        "p99": 106,
        "mean": 98.363,
        "max": 108,
    },
    "overlap_count": {
        "p1": 33,
        "p10": 23,
        "p50": 13,
        "p90": 6,
        "p99": 1,
        "mean": 13.877,
        "max": 0,
    },
    "mean_block_size": 4113,
    "mean_ref_size": 3842
}

# block_size (KB):       mean=5.374  p1=4.114 p10=4.505 p50=4.778  p90=6.457  p99=12.823
# num_transactions:      mean=0.855 p1=0.000 p10=0.000 p50=0.000  p90=2.000  p99=7.000
# actual num_tx:         mean=0.851 p1=0.000 p10=0.000 p50=0.000  p90=2.000  p99=7.000
# tx_pl_size_per_b (KB): mean=763.496
# tx_size:               mean=702.397  p1=332.000 p10=332.000 p50=340.000  p90=1235.000  p99=2027.000
# num_references:        mean=114.453  p1=100.000 p10=110.000 p50=115.000  p90=119.000  p99=121.000 max=128.000
# references_size (KB):  mean=4.471  p1=3.906 p10=4.297 p50=4.492  p90=4.648  p99=4.727
# % of total:            mean=0.88475 p1=0.348 p10=0.693 p50=0.963  p90=0.972  p99=0.973
# overlap:               mean=11.310 p1=2.000 p10=6.000 p50=10.000  p90=18.000  p99=29.000 max=73.000
# bitmap-size:           mean=8.841 p1=8.000 p10=8.000 p50=8.000  p90=8.000  p99=18.000 max=18.000
# 1171000 rounds.

data_set_2 = {
    "max_participants": 128,
    "num_references": {
        "p1": 100,
        "p10": 110,
        "p50": 115,
        "p90": 119,
        "p99": 121,
        "mean": 114.453,
        "max": 128,
    },
    "overlap_count": {
        "p1": 29,
        "p10": 18,
        "p50": 10,
        "p90": 6,
        "p99": 2,
        "mean": 11.310,
        "max": 0,
    },
    "mean_block_size": 5374,
    "mean_ref_size": 4471,
    "tx_pl_size_per_b": 763.496,
    "mean_tx_size": 702.397
}

def build_table(data) -> pd.DataFrame:
    rows = []
    for label, n in data["num_references"].items():
        rows.append({
            "percentile": label,
            "num_references": n,
            "sota_bytes": 40 * n,
            "bitset_bytes": HASH_SIZE_BYTES + math.ceil(data["max_participants"] / 8),
            "missing_ids_bytes": HASH_SIZE_BYTES + SHORT_SIZE_BYTES * (data["max_participants"] - n),
            "prev_delta_bytes": HASH_SIZE_BYTES + SHORT_SIZE_BYTES * data["overlap_count"][label],
            "compressed_bitset_bytes": HASH_SIZE_BYTES + math.ceil(data["max_participants"] / 8 / 2),
            "mean_block_size": data["mean_block_size"],
            "mean_basic_block_size": data["mean_block_size"] - data["mean_ref_size"]
        })

    df = pd.DataFrame(rows)

    for col in ["sota", "bitset", "missing_ids", "prev_delta", "compressed_bitset"]:
        df[f"{col}_kb"] = df[f"{col}_bytes"] / 1024

    for col in ["bitset", "missing_ids", "prev_delta", "compressed_bitset"]:
        df[f"{col}_reduction_pct"] = (1 - (df[f"{col}_bytes"] + df["mean_basic_block_size"]) / (df["mean_basic_block_size"] + df["sota_bytes"])) * 100

    return df.sort_values("num_references").reset_index(drop=True)

def save(fig, out_stem: str):
    fig.show()
    fig.savefig(f"{out_stem}.pdf")
    plt.close(fig)

def build_growth_table() -> pd.DataFrame:
    # Overhead outside of tx payload -- held fixed while we vary tx throughput.
    stable_overhead = (data_set_2["mean_block_size"] - data_set_2["mean_ref_size"]
                        - data_set_2["tx_pl_size_per_b"])

    rows = []
    target_tx_s = [0.333 * 108 * 13.94, 0.851 * 128 * 13.55, 15000, 30000, 60000, 120000]
    mp = 128  # stable max_participants, held fixed for comparability
    n = data_set_2["num_references"]["mean"]
    ov = data_set_2["overlap_count"]["mean"]

    for target in target_tx_s:
        tx_per_block = target / mp / 13.55

        extra_info = ""
        if target == 0.333 * 108 * 13.94:
            extra_info = "2025"
            tx_per_block = target / 108 / 13.94
        elif target == 0.851 * 128 * 13.55:
            extra_info = "2026"
            tx_per_block = target / 128 / 13.55

        rows.append({
            "qty": target.__floor__(),
            "extra_info": extra_info,
            "num_references": n,
            "max_participants": mp,
            "overlap_count": ov,
            "content_kb": (stable_overhead + tx_per_block * data_set_2["mean_tx_size"]) / 1024,
            "sota_kb": 40 * n / 1024,
            "bitset_kb": (HASH_SIZE_BYTES + math.ceil(mp / 8)) / 1024,
            "missing_ids_kb": (HASH_SIZE_BYTES + SHORT_SIZE_BYTES * (mp - n)) / 1024,
            "prev_delta_kb": (HASH_SIZE_BYTES + SHORT_SIZE_BYTES * ov) / 1024,
            "compressed_bitset_kb": (HASH_SIZE_BYTES + math.ceil(mp / 8 / 2)) / 1024,
        })
    return pd.DataFrame(rows)

def get_date(q):
    if q == (0.333*108*13.94).__floor__():
        return " (2025)"
    elif q == (0.851*128*13.55).__floor__():
        return " (2026)"
    return ""

def plot_stacked_growth(out_stem: str):
    df = build_growth_table()
    x = np.arange(len(df))
    keys = ["sota", "bitset", "missing_ids", "prev_delta", "compressed_bitset"]
    width = 0.15
    offsets = [-2, -1, 0, 1, 2]  # 5 slots for 5 keys

    fig, ax = plt.subplots(figsize=(11, 5.5))

    for i, (k, off) in enumerate(zip(keys, offsets)):
        xs = x + off * width
        hatch = "//" if k == "prev_delta" else ("xx" if k == "compressed_bitset" else None)

        ax.bar(xs, df["content_kb"].values, width - 0.01,
               color="#D9D9D9",
               label="Block content" if i == 0 else None)
        ax.bar(xs, df[f"{k}_kb"].values, width - 0.01,
               bottom=df["content_kb"].values,
               color=APPROACH_COLORS[k], hatch=hatch,
               label=APPROACH_LABELS[k])

    ax.set_xticks(x)

    ax.set_xticklabels([f"{q:g} tx/s" + get_date(q) for q in df["qty"]])

    ax.set_ylabel("Size in KB")
    ax.set_xlabel("Throughput tx/s")
    ax.legend(loc="upper left", fontsize=9, ncol=1)

    fig.tight_layout()
    save(fig, out_stem)

def build_overhead_scaling_table(validator_range):
    rows = []
    for mp in validator_range:
        rows.append({
            "max_participants": mp,
            "sota_overhead_bytes": 40 * ((2*mp)/3),
            "bitset_overhead_bytes": HASH_SIZE_BYTES + math.ceil(mp / 8),
        })
    return pd.DataFrame(rows)

def required_txs_for_overhead_fraction(overhead_bytes, target_fraction, n):
    stable_overhead = (data_set_2["mean_block_size"] - data_set_2["mean_ref_size"]
                       - data_set_2["tx_pl_size_per_b"])

    content_bytes_needed = overhead_bytes * (1 - target_fraction) / target_fraction
    tx_per_block = (content_bytes_needed - stable_overhead) / data_set_2["mean_tx_size"]
    return tx_per_block * 13.55 * n

def plot_overhead_scaling_log(out_stem: str, target_fractions=(0.01, 0.05, 0.10)):
    validator_range = np.arange(100, 1001, 50)
    df = build_overhead_scaling_table(validator_range)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    line_styles = {0.01: ":", 0.05: "--", 0.10: "-"}

    for f in target_fractions:
        sota_txs = df.apply(lambda row: required_txs_for_overhead_fraction(
            row["sota_overhead_bytes"], f, row["max_participants"]), axis=1)
        bitset_txs = df.apply(lambda row: required_txs_for_overhead_fraction(
            row["bitset_overhead_bytes"], f, row["max_participants"]), axis=1)

        ax.plot(df["max_participants"], sota_txs, linestyle=line_styles[f],
                 color=APPROACH_COLORS["sota"])
        ax.plot(df["max_participants"], bitset_txs, linestyle=line_styles[f],
                 color=APPROACH_COLORS["bitset"])

    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:,.0f}"))
    ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=range(2, 10), numticks=12))
    ax.yaxis.set_minor_formatter(FuncFormatter(lambda y, _: ""))

    ax.xaxis.set_major_locator(MultipleLocator(100))

    ax.set_xlabel("Number of validators")
    ax.set_ylabel("Throughput (tx/s)")

    fraction_handles = [
        Line2D([0], [0], color="black", linestyle=line_styles[f], label=f"\u2264{f*100:.0f}% overhead")
        for f in target_fractions
    ]
    fraction_legend = ax.legend(handles=fraction_handles, loc="upper left",
                                 fontsize=8, title="Target overhead")
    ax.add_artist(fraction_legend)

    approach_handles = [
        Line2D([0], [0], color=APPROACH_COLORS["sota"], label=APPROACH_LABELS["sota"]),
        Line2D([0], [0], color=APPROACH_COLORS["bitset"], label=APPROACH_LABELS["bitset"]),
    ]
    ax.legend(handles=approach_handles, loc="lower right", fontsize=8, title="Approach")

    fig.tight_layout()
    save(fig, out_stem)

def plot_reduction_pct(out_stem: str):
    df = build_table(data_set_1)

    sub = df.set_index("percentile")
    x = np.arange(len(sub))
    keys = ["bitset", "missing_ids", "prev_delta", "compressed_bitset"]

    fig, ax = plt.subplots(figsize=(11, 5))
    for k in keys:
        ls = "--" if k == "prev_delta" else "-"
        ax.plot(x, sub[f"{k}_reduction_pct"], marker="o", linestyle=ls,
                 label="2025 " + APPROACH_LABELS[k], color=APPROACH_COLORS[k])

    df = build_table(data_set_2)
    sub = df.set_index("percentile")

    for k in keys:
        ls = "--" if k == "prev_delta" else "-"
        ax.plot(x, sub[f"{k}_reduction_pct"], marker="s", linestyle=ls,
                label="2026 " + APPROACH_LABELS[k], color=APPROACH_COLORS[k])

    ax.set_xticks(x)
    ax.set_xticklabels(sub.index)
    ax.set_ylim(70, 100)
    ax.set_yticks(np.arange(0, 105, 10))

    ax.set_ylabel("Mean Block Size reduction vs. SOTA (%)")
    ax.set_xlabel("Distribution of References per Block")

    approach_handles = [
        Line2D([0], [0], color=APPROACH_COLORS[k],
               linestyle="--" if k == "prev_delta" else "-",
               label=APPROACH_LABELS[k])
        for k in keys
    ]
    approach_legend = ax.legend(handles=approach_handles, loc="lower right",
                                 fontsize=9, title="Approach")
    ax.add_artist(approach_legend)  # <- keeps this one alive past the next ax.legend() call

    year_handles = [
        Line2D([0], [0], color="black", marker="o", linestyle="", label="648 (2025)"),
        Line2D([0], [0], color="black", marker="s", linestyle="", label="1230 (2026)"),
    ]
    ax.legend(handles=year_handles, loc="lower left", fontsize=9, title="Epoch")

    fig.tight_layout()
    save(fig, out_stem)

def main(out_dir: str = "output"):
    os.makedirs(out_dir, exist_ok=True)
    
    plt.rcParams.update({
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "grid.linestyle": "--",
        "figure.dpi": 140,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
    })

    plot_stacked_growth(os.path.join(out_dir, "stacked_growth_projection"))
    plot_overhead_scaling_log(os.path.join(out_dir, "overhead_scaling"))

    plot_reduction_pct(os.path.join(out_dir, "reduction_percentage"))

    pd.set_option("display.width", 140)
    pd.set_option("display.float_format", lambda v: f"{v:,.3f}")

if __name__ == "__main__":
    main()