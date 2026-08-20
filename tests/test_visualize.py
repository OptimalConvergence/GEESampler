import csv

import numpy as np
import rasterio
from rasterio.transform import from_origin

from geesampler.visualize import plot_benchmark, plot_sample_pair


def test_static_validation_and_benchmark_figures(tmp_path):
    image = tmp_path / "image.tif"
    profile = {
        "driver": "GTiff",
        "height": 16,
        "width": 16,
        "count": 3,
        "dtype": "uint16",
        "crs": "EPSG:32632",
        "transform": from_origin(0, 160, 10, 10),
    }
    with rasterio.open(image, "w", **profile) as destination:
        destination.write(np.arange(3 * 16 * 16, dtype="uint16").reshape(3, 16, 16))
    preview = plot_sample_pair(image, tmp_path / "preview.png")
    assert preview.stat().st_size > 1000

    benchmark = tmp_path / "benchmark.csv"
    with benchmark.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "case",
                "bandwidth_mib_per_second",
                "samples_per_second",
                "eecu_per_success",
                "compute_pixels_p95_seconds",
            ],
        )
        writer.writeheader()
        for index in range(8):
            writer.writerow(
                {
                    "case": f"case-{index}",
                    "bandwidth_mib_per_second": index + 1,
                    "samples_per_second": (index + 1) / 2,
                    "eecu_per_success": 10 - index,
                    "compute_pixels_p95_seconds": 8 - index / 2,
                }
            )
    output = plot_benchmark(benchmark, tmp_path / "benchmark.png")
    assert output.stat().st_size > 1000
