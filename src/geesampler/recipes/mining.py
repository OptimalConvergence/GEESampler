from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from ..models import SampleRecord
from ..sources import FileSampleSource

MINING_STAC = "https://iiasa.blob.core.windows.net/collections/globalmininglanduse_2019.json"
MINING_DOI = "https://doi.org/10.1594/PANGAEA.942325"


def mining_records(path: str | Path, *, limit: int = 8, seed: int = 42) -> Iterator[SampleRecord]:
    """Select one 0.1-8 km² polygon per country, deterministically."""
    source = FileSampleSource(path, id_column="fid", date_column=None)
    candidates = [
        item for item in source.records() if 0.1 <= float(item.properties.get("AREA", -1)) <= 8.0
    ]
    candidates.sort(
        key=lambda item: (
            str(item.properties.get("ISO3_CODE", "")),
            (sum(ord(char) for char in item.sample_id) + seed) % 1_000_003,
        )
    )
    chosen: list[SampleRecord] = []
    countries: set[str] = set()
    for item in candidates:
        country = str(item.properties.get("ISO3_CODE", ""))
        if not country or country in countries:
            continue
        countries.add(country)
        chosen.append(
            SampleRecord(
                f"mining-{item.sample_id}",
                item.geometry,
                datetime(2019, 6, 1, tzinfo=timezone.utc),
                {**item.properties, "DatasetDOI": MINING_DOI, "Class": 1},
            )
        )
        if len(chosen) == limit:
            break
    yield from chosen
