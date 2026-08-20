import json
from datetime import timezone

import pytest

from geesampler.config import SamplerConfig, load_callable
from geesampler.grid import compute_grid, utm_epsg
from geesampler.models import PatchGrid, SampleRecord, parse_datetime
from geesampler.sources import FileSampleSource, _record


def test_parse_epoch_millis_and_grid_dimensions():
    parsed = parse_datetime(1_609_459_200_000)
    assert parsed.year == 2021
    assert parsed.tzinfo == timezone.utc
    grid = compute_grid({"type": "Point", "coordinates": [145.5, -37.5]}, PatchGrid())
    assert grid.width == grid.height == 336
    assert grid.scale_x == 10
    assert grid.scale_y == -10
    assert grid.crs == utm_epsg(145.5, -37.5) == "EPSG:32755"
    assert grid.translate_x + 1680 > 0


def test_csv_and_geojson_sources(tmp_path):
    csv_path = tmp_path / "samples.csv"
    csv_path.write_text("SampleID,Date,Lon,Lat\na,2021-01-02,3,4\n", encoding="utf-8")
    records = list(FileSampleSource(csv_path).records())
    assert records[0].sample_id == "a"
    assert records[0].geometry == {"type": "Point", "coordinates": [3.0, 4.0]}

    geojson_path = tmp_path / "samples.geojson"
    geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"SampleID": "p", "Date": "2020-01-01"},
                        "geometry": {"type": "Point", "coordinates": [1, 2]},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    assert next(iter(FileSampleSource(geojson_path).records())).sample_id == "p"


def test_record_normalizes_timestamp_properties():
    import pandas as pd

    record = _record(
        {"SampleID": 7, "observed": pd.Timestamp("2019-06-01")},
        {"type": "Point", "coordinates": [0, 0]},
        "SampleID",
        None,
    )
    assert record.properties["observed"] == "2019-06-01T00:00:00"


def test_yaml_environment_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_GEE_PROJECT", "project-x")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n  project: ${TEST_GEE_PROJECT}\nrun:\n  output_dir: ./out\n"
        "  monitoring: {enabled: false}\n",
        encoding="utf-8",
    )
    config = SamplerConfig.from_yaml(config_path)
    assert config.auth.project == "project-x"
    assert config.run.eecu.enabled is False
    assert load_callable("geesampler.recipes.sentinel2:polygon_mask").__name__ == "polygon_mask"


def test_yaml_catalog_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_GEE_PROJECT", "project-x")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
auth:
  project: ${TEST_GEE_PROJECT}
run:
  output_dir: ./out
  monitoring: {enabled: false}
catalog:
  enabled: true
  path: ./cache/s2.sqlite
  metadata_cloud_max: 12
  cloud: {mode: metadata_only, threshold: 0.65}
""",
        encoding="utf-8",
    )
    config = SamplerConfig.from_yaml(config_path)
    assert config.catalog is not None
    assert config.catalog.path.name == "s2.sqlite"
    assert config.catalog.resolver.metadata_cloud_max == 12
    assert config.catalog.resolver.cloud_mode == "metadata_only"
    assert config.catalog.resolver.qa_threshold == 0.65


def test_sample_rejects_unsupported_geometry():
    with pytest.raises(ValueError, match="Unsupported"):
        SampleRecord("x", {"type": "LineString", "coordinates": []})
