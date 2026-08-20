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
    """Compare payload throughput and reported EECU/sample across benchmark cases."""
    import matplotlib.pyplot as plt
    import numpy as np

    with Path(csv_path).open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("Benchmark CSV is empty")
    labels = [row["case"] for row in rows]
    bandwidth = [float(row["bandwidth_mib_per_second"]) for row in rows]
    eecu = [float(row["eecu_per_success"] or "nan") for row in rows]
    order = np.argsort(bandwidth)
    labels = [labels[index] for index in order]
    bandwidth = [bandwidth[index] for index in order]
    eecu = [eecu[index] for index in order]
    y = np.arange(len(labels))

    figure, axes = plt.subplots(1, 2, figsize=(14, max(5, len(labels) * 0.42)), sharey=True)
    axes[0].barh(y, bandwidth, color=BLUE, edgecolor="#1D4ED8")
    axes[0].set_title("Payload bandwidth by configuration", color=INK)
    axes[0].set_xlabel("MiB/s (downloaded payload / wall time)")
    axes[0].set_yticks(y, labels)
    axes[1].barh(y, eecu, color="#F7E7B2", edgecolor=GOLD)
    axes[1].set_title("Reported EECU per successful sample", color=INK)
    axes[1].set_xlabel("EECU-seconds/sample")
    for axis in axes:
        axis.grid(axis="x", color=GRID, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
    figure.suptitle("GEESampler benchmark comparison", color=INK, fontsize=15)
    figure.text(
        0.5,
        0.01,
        "EECU is workload-tagged Cloud Monitoring usage and may lag completed downloads.",
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
