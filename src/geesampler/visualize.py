from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path

BLUE = "#2563EB"
GOLD = "#D4A72C"
INK = "#1F2937"
GRID = "#E5E7EB"


def _stretch(channel):
    import numpy as np

    valid = channel[np.isfinite(channel)]
    if not len(valid):
        return np.zeros_like(channel, dtype="float32")
    low, high = np.percentile(valid, [2, 98])
    if high <= low:
        high = low + 1
    return np.clip((channel - low) / (high - low), 0, 1)


def plot_sample_pair(
    image_path: str | Path,
    output_path: str | Path,
    *,
    mask_path: str | Path | None = None,
    rgb_band_indexes: Sequence[int] = (3, 2, 1),
    title: str = "Downloaded Sentinel-2 sample",
) -> Path:
    """Export an RGB and optional aligned-mask validation figure."""
    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio

    image_path = Path(image_path)
    output_path = Path(output_path)
    with rasterio.open(image_path) as source:
        rgb = np.stack([_stretch(source.read(index)) for index in rgb_band_indexes], axis=-1)
        extent = [source.bounds.left, source.bounds.right, source.bounds.bottom, source.bounds.top]
        crs = source.crs
    columns = 2 if mask_path else 1
    figure, axes = plt.subplots(1, columns, figsize=(6 * columns, 5), squeeze=False)
    axis = axes[0, 0]
    axis.imshow(rgb, extent=extent)
    axis.set_title("Sentinel-2 RGB", color=INK)
    axis.set_xlabel(f"Projected coordinate ({crs})")
    axis.set_ylabel("Projected coordinate")
    if mask_path:
        with rasterio.open(mask_path) as source:
            mask = source.read(1)
        axes[0, 1].imshow(mask, cmap="gray", vmin=0, vmax=1, extent=extent)
        axes[0, 1].set_title("Aligned polygon mask", color=INK)
        axes[0, 1].set_xlabel("Projected coordinate")
        axes[0, 1].set_ylabel("Projected coordinate")
    figure.suptitle(title, color=INK, fontsize=14)
    figure.text(
        0.5,
        0.01,
        "RGB uses a per-patch 2–98% display stretch; exported pixel values are unchanged.",
        ha="center",
        color="#6B7280",
        fontsize=9,
    )
    figure.tight_layout(rect=[0, 0.04, 1, 0.95])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(figure)
    return output_path


def plot_benchmark(csv_path: str | Path, output_path: str | Path) -> Path:
    """Compare throughput, cost, and tail latency across benchmark cases."""
    import matplotlib.pyplot as plt
    import numpy as np

    with Path(csv_path).open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("Benchmark CSV is empty")
    labels = [row["case"] for row in rows]
    bandwidth = [float(row["bandwidth_mib_per_second"]) for row in rows]
    sample_rate = [float(row.get("samples_per_second") or "nan") for row in rows]
    eecu = [float(row["eecu_per_success"] or "nan") for row in rows]
    latency_key = (
        "compute_pixels_p95_seconds"
        if any(row.get("compute_pixels_p95_seconds") for row in rows)
        else "planning_seconds"
    )
    latency = [float(row.get(latency_key) or "nan") for row in rows]
    order = np.argsort(bandwidth if all(np.isnan(value) for value in sample_rate) else sample_rate)
    labels = [labels[index] for index in order]
    bandwidth = [bandwidth[index] for index in order]
    sample_rate = [sample_rate[index] for index in order]
    eecu = [eecu[index] for index in order]
    latency = [latency[index] for index in order]
    y = np.arange(len(labels))

    figure, axes_grid = plt.subplots(
        2,
        2,
        figsize=(16, max(7, len(labels) * 0.65)),
        sharey=True,
    )
    axes = axes_grid.ravel()
    panels = (
        (sample_rate, "Successful sample throughput", "samples/s", BLUE, "#1D4ED8"),
        (bandwidth, "Downloaded payload throughput", "MiB/s", "#BED7F7", BLUE),
        (eecu, "Reported compute per successful sample", "EECU-seconds/sample", "#F7E7B2", GOLD),
        (
            latency,
            "Pixel-request tail latency" if latency_key.startswith("compute") else "Planning time",
            "p95 seconds" if latency_key.startswith("compute") else "seconds/run",
            "#F6C9A8",
            "#C45A16",
        ),
    )
    for axis, (values, title, xlabel, color, edge) in zip(axes, panels):
        axis.barh(y, values, color=color, edgecolor=edge)
        axis.set_title(title, color=INK)
        axis.set_xlabel(xlabel)
        axis.set_yticks(y, labels)
        axis.grid(axis="x", color=GRID, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
    figure.suptitle("GEESampler sampling-efficiency benchmark", color=INK, fontsize=15)
    figure.text(
        0.5,
        0.01,
        "Same sampled workload per case. EECU is workload-tagged Cloud Monitoring usage and may lag.",
        ha="center",
        color="#6B7280",
        fontsize=9,
    )
    figure.tight_layout(rect=[0, 0.04, 1, 0.95])
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(figure)
    return output_path
