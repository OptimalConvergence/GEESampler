from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_millis(value: datetime | float) -> int:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(normalized.timestamp() * 1000)
    return int(value)


def _bounds(geometry: Mapping[str, Any] | None) -> tuple[float, float, float, float] | None:
    if not geometry:
        return None
    coordinates = geometry.get("coordinates")
    points: list[tuple[float, float]] = []

    def visit(value: Any) -> None:
        if (
            isinstance(value, Sequence)
            and len(value) >= 2
            and isinstance(value[0], (int, float))
            and isinstance(value[1], (int, float))
        ):
            points.append((float(value[0]), float(value[1])))
            return
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for item in value:
                visit(item)

    visit(coordinates)
    if not points:
        return None
    xs, ys = zip(*points)
    return min(xs), min(ys), max(xs), max(ys)


@dataclass(frozen=True)
class SceneRecord:
    collection: str
    asset_id: str
    scene_id: str
    acquired_millis: int
    mgrs_tile: str
    cloud_percentage: float | None = None
    bbox: tuple[float, float, float, float] | None = None
    properties: Mapping[str, Any] | None = None

    @property
    def acquired_at(self) -> datetime:
        return datetime.fromtimestamp(self.acquired_millis / 1000.0, tz=timezone.utc)


@dataclass(frozen=True)
class CatalogStats:
    scenes: int
    tiles: int
    coverage_intervals: int
    patch_quality_rows: int
    size_bytes: int
    oldest_scene: str | None
    newest_scene: str | None


class S2SceneCatalog:
    """Incremental Sentinel-2 metadata catalog backed by SQLite and RTree."""

    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        with self._write_lock, self._connect() as connection:
            try:
                connection.execute(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS _rtree_probe USING rtree(id,minx,maxx,miny,maxy)"
                )
                connection.execute("DROP TABLE _rtree_probe")
            except sqlite3.OperationalError as exc:
                raise RuntimeError("SQLite was built without the RTree extension") from exc
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scenes (
                    id INTEGER PRIMARY KEY,
                    collection TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    scene_id TEXT NOT NULL,
                    acquired_millis INTEGER NOT NULL,
                    mgrs_tile TEXT NOT NULL,
                    cloud_percentage REAL,
                    properties_json TEXT NOT NULL DEFAULT '{}',
                    ingested_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    UNIQUE(collection, asset_id)
                );
                CREATE INDEX IF NOT EXISTS scenes_tile_time_cloud
                    ON scenes(collection, mgrs_tile, acquired_millis, cloud_percentage);
                CREATE INDEX IF NOT EXISTS scenes_index
                    ON scenes(collection, scene_id);
                CREATE VIRTUAL TABLE IF NOT EXISTS scene_rtree USING rtree(
                    id, minx, maxx, miny, maxy
                );
                CREATE TABLE IF NOT EXISTS coverage (
                    collection TEXT NOT NULL,
                    mgrs_tile TEXT NOT NULL,
                    start_millis INTEGER NOT NULL,
                    end_millis INTEGER NOT NULL,
                    fetched_at TEXT NOT NULL,
                    CHECK(start_millis < end_millis),
                    UNIQUE(collection, mgrs_tile, start_millis, end_millis)
                );
                CREATE INDEX IF NOT EXISTS coverage_lookup
                    ON coverage(collection, mgrs_tile, start_millis, end_millis);
                CREATE TABLE IF NOT EXISTS patch_quality (
                    collection TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    grid_hash TEXT NOT NULL,
                    qa_band TEXT NOT NULL,
                    qa_threshold REAL NOT NULL,
                    min_clear_fraction REAL NOT NULL,
                    clear_fraction REAL NOT NULL,
                    accepted INTEGER NOT NULL,
                    evaluated_at TEXT NOT NULL,
                    PRIMARY KEY(
                        collection, asset_id, grid_hash, qa_band,
                        qa_threshold, min_clear_fraction
                    )
                );
                CREATE TABLE IF NOT EXISTS imports (
                    source_path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    imported_at TEXT NOT NULL
                );
                """
            )
            connection.execute(f"PRAGMA user_version={self.SCHEMA_VERSION}")

    @staticmethod
    def _upsert_scene(connection: sqlite3.Connection, scene: SceneRecord, now: str) -> None:
        properties = json.dumps(dict(scene.properties or {}), sort_keys=True, default=str)
        connection.execute(
            """
            INSERT INTO scenes (
                collection, asset_id, scene_id, acquired_millis, mgrs_tile,
                cloud_percentage, properties_json, ingested_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(collection, asset_id) DO UPDATE SET
                scene_id=excluded.scene_id,
                acquired_millis=excluded.acquired_millis,
                mgrs_tile=excluded.mgrs_tile,
                cloud_percentage=excluded.cloud_percentage,
                properties_json=excluded.properties_json,
                last_seen_at=excluded.last_seen_at
            """,
            (
                scene.collection,
                scene.asset_id,
                scene.scene_id,
                scene.acquired_millis,
                scene.mgrs_tile,
                scene.cloud_percentage,
                properties,
                now,
                now,
            ),
        )
        row = connection.execute(
            "SELECT id FROM scenes WHERE collection=? AND asset_id=?",
            (scene.collection, scene.asset_id),
        ).fetchone()
        if scene.bbox is not None:
            minx, miny, maxx, maxy = scene.bbox
            connection.execute(
                "INSERT OR REPLACE INTO scene_rtree(id,minx,maxx,miny,maxy) VALUES (?,?,?,?,?)",
                (row[0], minx, maxx, miny, maxy),
            )

    @staticmethod
    def _mark_coverage_tx(
        connection: sqlite3.Connection,
        collection: str,
        tile: str,
        start_millis: int,
        end_millis: int,
        fetched_at: str,
    ) -> None:
        overlapping = connection.execute(
            """
            SELECT start_millis, end_millis FROM coverage
            WHERE collection=? AND mgrs_tile=?
              AND end_millis >= ? AND start_millis <= ?
            """,
            (collection, tile, start_millis, end_millis),
        ).fetchall()
        merged_start = min([start_millis, *(row[0] for row in overlapping)])
        merged_end = max([end_millis, *(row[1] for row in overlapping)])
        if overlapping:
            connection.execute(
                """
                DELETE FROM coverage WHERE collection=? AND mgrs_tile=?
                  AND end_millis >= ? AND start_millis <= ?
                """,
                (collection, tile, start_millis, end_millis),
            )
        connection.execute(
            """
            INSERT INTO coverage(collection,mgrs_tile,start_millis,end_millis,fetched_at)
            VALUES (?,?,?,?,?)
            """,
            (collection, tile, merged_start, merged_end, fetched_at),
        )

    def upsert_and_mark_coverage(
        self,
        scenes: Iterable[SceneRecord],
        *,
        collection: str,
        tiles: Iterable[str],
        start: datetime | int,
        end: datetime | int,
    ) -> None:
        start_millis, end_millis = _utc_millis(start), _utc_millis(end)
        if start_millis >= end_millis:
            raise ValueError("coverage start must precede end")
        now = datetime.now(timezone.utc).isoformat()
        with self._write_lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for scene in scenes:
                self._upsert_scene(connection, scene, now)
            for tile in sorted(set(tiles)):
                self._mark_coverage_tx(connection, collection, tile, start_millis, end_millis, now)
            connection.commit()

    def missing_intervals(
        self,
        collection: str,
        tile: str,
        start: datetime | int,
        end: datetime | int,
        *,
        fetched_after: datetime | None = None,
    ) -> list[tuple[int, int]]:
        start_millis, end_millis = _utc_millis(start), _utc_millis(end)
        query = """
            SELECT start_millis, end_millis FROM coverage
            WHERE collection=? AND mgrs_tile=? AND end_millis>? AND start_millis<?
        """
        parameters: list[Any] = [collection, tile, start_millis, end_millis]
        if fetched_after is not None:
            query += " AND fetched_at>=?"
            query_fresh = (
                fetched_after
                if fetched_after.tzinfo
                else fetched_after.replace(tzinfo=timezone.utc)
            )
            parameters.append(query_fresh.isoformat())
        query += " ORDER BY start_millis"
        with self._connect() as connection:
            covered = connection.execute(query, parameters).fetchall()
        missing: list[tuple[int, int]] = []
        cursor = start_millis
        for row in covered:
            covered_start = max(start_millis, int(row[0]))
            covered_end = min(end_millis, int(row[1]))
            if covered_start > cursor:
                missing.append((cursor, covered_start))
            cursor = max(cursor, covered_end)
        if cursor < end_millis:
            missing.append((cursor, end_millis))
        return missing

    def query(
        self,
        *,
        collection: str,
        tiles: Iterable[str],
        start: datetime | int,
        end: datetime | int,
        max_cloud_percentage: float | None,
        bbox: tuple[float, float, float, float] | None = None,
    ) -> list[SceneRecord]:
        tile_values = sorted(set(tiles))
        if not tile_values:
            return []
        placeholders = ",".join("?" for _ in tile_values)
        parameters: list[Any] = [collection, *tile_values, _utc_millis(start), _utc_millis(end)]
        spatial_join = " LEFT JOIN scene_rtree r ON r.id=s.id"
        spatial_where = ""
        if bbox is not None:
            minx, miny, maxx, maxy = bbox
            spatial_where = (
                " AND (r.id IS NULL OR (r.minx<=? AND r.maxx>=? AND r.miny<=? AND r.maxy>=?))"
            )
            parameters.extend((maxx, minx, maxy, miny))
        cloud_where = ""
        if max_cloud_percentage is not None:
            cloud_where = " AND s.cloud_percentage<=?"
            parameters.append(float(max_cloud_percentage))
        sql = f"""
            SELECT s.*, r.minx AS bbox_minx, r.miny AS bbox_miny,
                   r.maxx AS bbox_maxx, r.maxy AS bbox_maxy
            FROM scenes s{spatial_join}
            WHERE s.collection=? AND s.mgrs_tile IN ({placeholders})
              AND s.acquired_millis>=? AND s.acquired_millis<?
              {spatial_where}{cloud_where}
        """
        with self._connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
            result = []
            for row in rows:
                bbox_row = (
                    (
                        row["bbox_minx"],
                        row["bbox_miny"],
                        row["bbox_maxx"],
                        row["bbox_maxy"],
                    )
                    if row["bbox_minx"] is not None
                    else None
                )
                result.append(
                    SceneRecord(
                        collection=row["collection"],
                        asset_id=row["asset_id"],
                        scene_id=row["scene_id"],
                        acquired_millis=row["acquired_millis"],
                        mgrs_tile=row["mgrs_tile"],
                        cloud_percentage=row["cloud_percentage"],
                        bbox=bbox_row,
                        properties=json.loads(row["properties_json"]),
                    )
                )
        return result

    def quality(
        self,
        scene: SceneRecord,
        grid_hash: str,
        qa_band: str,
        qa_threshold: float,
        min_clear_fraction: float,
    ) -> tuple[float, bool] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT clear_fraction, accepted FROM patch_quality
                WHERE collection=? AND asset_id=? AND grid_hash=?
                  AND qa_band=? AND qa_threshold=? AND min_clear_fraction=?
                """,
                (
                    scene.collection,
                    scene.asset_id,
                    grid_hash,
                    qa_band,
                    qa_threshold,
                    min_clear_fraction,
                ),
            ).fetchone()
        return (float(row[0]), bool(row[1])) if row else None

    def record_quality(
        self,
        scene: SceneRecord,
        grid_hash: str,
        qa_band: str,
        qa_threshold: float,
        min_clear_fraction: float,
        clear_fraction: float,
        accepted: bool,
    ) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO patch_quality(
                    collection,asset_id,grid_hash,qa_band,qa_threshold,
                    min_clear_fraction,clear_fraction,accepted,evaluated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(
                    collection,asset_id,grid_hash,qa_band,
                    qa_threshold,min_clear_fraction
                )
                DO UPDATE SET clear_fraction=excluded.clear_fraction,
                              accepted=excluded.accepted,
                              evaluated_at=excluded.evaluated_at
                """,
                (
                    scene.collection,
                    scene.asset_id,
                    grid_hash,
                    qa_band,
                    qa_threshold,
                    min_clear_fraction,
                    clear_fraction,
                    int(accepted),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def import_geelinker(
        self,
        directory: str | Path,
        *,
        collection: str,
        start: datetime,
        end: datetime,
    ) -> tuple[int, int]:
        """Import GEELinker's per-MGRS ImageCollection JSON files once."""
        imported_files = 0
        imported_scenes = 0
        for path in sorted(Path(directory).expanduser().glob("*.json")):
            stat = path.stat()
            with self._connect() as connection:
                existing = connection.execute(
                    "SELECT size_bytes,modified_ns FROM imports WHERE source_path=?",
                    (str(path.resolve()),),
                ).fetchone()
            if existing and tuple(existing) == (stat.st_size, stat.st_mtime_ns):
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            tile = path.stem
            scenes: list[SceneRecord] = []
            for feature in payload.get("features", []):
                properties = feature.get("properties", {})
                scene_tile = str(properties.get("MGRS_TILE") or tile)
                asset_id = str(feature.get("id") or properties.get("system:id") or "")
                scene_id = str(properties.get("system:index") or asset_id.rsplit("/", 1)[-1])
                acquired = properties.get("system:time_start")
                if not asset_id or acquired in (None, ""):
                    continue
                scenes.append(
                    SceneRecord(
                        collection=collection,
                        asset_id=asset_id,
                        scene_id=scene_id,
                        acquired_millis=int(acquired),
                        mgrs_tile=scene_tile,
                        cloud_percentage=properties.get("CLOUDY_PIXEL_PERCENTAGE"),
                        bbox=_bounds(feature.get("geometry")),
                        properties={
                            key: properties[key]
                            for key in (
                                "PRODUCT_ID",
                                "GRANULE_ID",
                                "SPACECRAFT_NAME",
                                "SENSING_ORBIT_NUMBER",
                            )
                            if key in properties
                        },
                    )
                )
            self.upsert_and_mark_coverage(
                scenes, collection=collection, tiles={tile}, start=start, end=end
            )
            with self._write_lock, self._connect() as connection:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO imports(source_path,size_bytes,modified_ns,imported_at)
                    VALUES (?,?,?,?)
                    """,
                    (
                        str(path.resolve()),
                        stat.st_size,
                        stat.st_mtime_ns,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            imported_files += 1
            imported_scenes += len(scenes)
        return imported_files, imported_scenes

    def stats(self) -> CatalogStats:
        with self._connect() as connection:
            scenes, tiles, oldest, newest = connection.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT mgrs_tile),
                       MIN(acquired_millis), MAX(acquired_millis) FROM scenes
                """
            ).fetchone()
            coverage = connection.execute("SELECT COUNT(*) FROM coverage").fetchone()[0]
            quality = connection.execute("SELECT COUNT(*) FROM patch_quality").fetchone()[0]

        def iso(value: int | None) -> str | None:
            return (
                datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()
                if value is not None
                else None
            )

        return CatalogStats(
            scenes=int(scenes),
            tiles=int(tiles),
            coverage_intervals=int(coverage),
            patch_quality_rows=int(quality),
            size_bytes=self.path.stat().st_size if self.path.exists() else 0,
            oldest_scene=iso(oldest),
            newest_scene=iso(newest),
        )


__all__ = ["CatalogStats", "S2SceneCatalog", "SceneRecord"]
