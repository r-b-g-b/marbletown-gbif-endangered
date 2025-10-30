from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
import time

from diskcache import Cache
import polars as pl
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
GBIF_SEARCH_URL = "https://api.gbif.org/v1/occurrence/search"
USER_AGENT = "nys-gbif-endangered/0.1 (codex-cli)"
STATUS_CSV = Path("data/nynhp-status-list_2025-10-29.csv")
OUTPUT_PATH = Path("data/marbletown_gbif_occurrences.parquet")
CACHE_DIR = Path(".cache") / "nominatim"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
cache = Cache(str(CACHE_DIR))

GBIF_MAX_RETRIES = 5
PAGE_DELAY_SECONDS = 0.2


@dataclass
class PipelineResult:
    bounding_box: tuple[float, float, float, float]
    geometry_wkt: str
    bbox_wkt: str
    occurrences_total: int
    with_status_count: int
    dataframe: pl.DataFrame
    output_path: Path


@cache.memoize(expire=7 * 24 * 3600)
def fetch_marbletown_boundary() -> tuple[list[float], dict]:
    params = {
        "format": "jsonv2",
        "polygon_geojson": 1,
        "country": "United States",
        "state": "New York",
        "county": "Ulster County",
        "city": "Marbletown",
        "namedetails": 0,
        "addressdetails": 0,
    }
    response = requests.get(NOMINATIM_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    results = response.json()
    if not results:
        raise RuntimeError("No boundary information returned by Nominatim.")

    admin_result = next((item for item in results if item.get("type") == "administrative"), results[0])

    try:
        bounding_box = [float(value) for value in admin_result["boundingbox"]]
        polygon_geojson = admin_result["geojson"]
    except (KeyError, ValueError) as exc:
        raise RuntimeError("Unexpected boundary payload from Nominatim.") from exc

    return bounding_box, polygon_geojson


def bounding_box_to_wkt(bounding_box: Iterable[float]) -> str:
    south, north, west, east = bounding_box
    return (
        "POLYGON(("
        f"{west} {south}, "
        f"{east} {south}, "
        f"{east} {north}, "
        f"{west} {north}, "
        f"{west} {south}"
        "))"
    )


def _ensure_closed_ring(coords: Sequence[Sequence[float]]) -> list[tuple[float, float]]:
    if not coords:
        return []
    ring = [(float(lon), float(lat)) for lon, lat in coords]
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def geojson_polygon_to_wkt(geojson: dict) -> str:
    geometry_type = geojson.get("type")
    coordinates = geojson.get("coordinates")
    if geometry_type not in {"Polygon", "MultiPolygon"} or not coordinates:
        raise ValueError(f"Unsupported GeoJSON geometry: {geometry_type}")

    if geometry_type == "Polygon":
        rings = []
        for ring_coords in coordinates:
            ring = _ensure_closed_ring(ring_coords)
            rings.append("(" + ", ".join(f"{lon} {lat}" for lon, lat in ring) + ")")
        return f"POLYGON({', '.join(rings)})"

    polygons = []
    for polygon_coords in coordinates:
        rings = []
        for ring_coords in polygon_coords:
            ring = _ensure_closed_ring(ring_coords)
            rings.append("(" + ", ".join(f"{lon} {lat}" for lon, lat in ring) + ")")
        polygons.append("(" + ", ".join(rings) + ")")
    return f"MULTIPOLYGON({', '.join(polygons)})"


def _retry_delay_from_response(response: requests.Response) -> float:
    header_value = response.headers.get("Retry-After")
    if header_value:
        try:
            return max(float(header_value), 1.0)
        except ValueError:
            return 1.0
    return 1.0


@retry(
    stop=stop_after_attempt(GBIF_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type(requests.HTTPError),
    reraise=True,
)
def _request_gbif_page(session: requests.Session, params: dict) -> dict:
    response = session.get(GBIF_SEARCH_URL, params=params, timeout=60)
    if response.status_code == 429:
        time.sleep(_retry_delay_from_response(response))
    response.raise_for_status()
    return response.json()


def fetch_gbif_occurrences(
    geometry_wkt: str,
    threat_statuses: Sequence[str] | None = None,
    page_limit: int = 300,
) -> list[dict]:
    if threat_statuses is None:
        threat_statuses = ("CR", "EN", "VU", "NT")

    unique_occurrences: dict[int | str, dict] = {}

    with requests.Session() as session:
        session.headers.update({"User-Agent": USER_AGENT})

        for status in threat_statuses:
            offset = 0
            while True:
                params = {
                    "geometry": geometry_wkt,
                    "iucnRedListCategory": status,
                    "limit": page_limit,
                    "offset": offset,
                }
                payload = _request_gbif_page(session, params)

                batch = payload.get("results", [])
                for record in batch:
                    record_id = record.get("gbifID") or f"{status}-{offset}-{len(unique_occurrences)}"
                    unique_occurrences[record_id] = record

                if not batch or payload.get("endOfRecords"):
                    break

                offset += page_limit
                time.sleep(PAGE_DELAY_SECONDS)

    return list(unique_occurrences.values())


def load_status_reference() -> pl.DataFrame:
    columns_to_keep = [
        "Scientific name",
        "Primary common name",
        "Global conservation status rank",
        "State conservation status rank",
        "Federal protection",
        "State protection",
        "Species of greatest conservation need",
    ]
    status_df = pl.read_csv(STATUS_CSV, columns=columns_to_keep)
    status_df = status_df.rename({"Scientific name": "scientificName"})
    return status_df.with_columns(pl.col("scientificName").alias("matchName"))


def build_occurrence_frame(occurrences: list[dict]) -> pl.DataFrame:
    preferred_columns = [
        "gbifID",
        "scientificName",
        "vernacularName",
        "decimalLatitude",
        "decimalLongitude",
        "eventDate",
        "basisOfRecord",
        "datasetKey",
        "datasetName",
        "occurrenceStatus",
        "iucnRedListCategory",
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "recordedBy",
        "identifiedBy",
        "institutionCode",
        "catalogNumber",
        "references",
    ]

    if not occurrences:
        return pl.DataFrame({column: [] for column in preferred_columns})

    occurrence_df = pl.DataFrame(occurrences)
    if "scientificName" not in occurrence_df.columns:
        occurrence_df = occurrence_df.with_columns(pl.lit(None).alias("scientificName"))

    existing_columns = [column for column in preferred_columns if column in occurrence_df.columns]
    if existing_columns:
        occurrence_df = occurrence_df.select(existing_columns)

    return occurrence_df


def tag_occurrences_with_status(occurrence_df: pl.DataFrame, status_df: pl.DataFrame) -> pl.DataFrame:
    if occurrence_df.height == 0:
        return occurrence_df.with_columns(pl.lit(False).alias("has_nynhp_status"))

    decorated = occurrence_df.with_columns(
        pl.when(pl.col("species").is_not_null() & (pl.col("species") != ""))
        .then(pl.col("species"))
        .otherwise(pl.col("scientificName"))
        .alias("matchName")
    )

    merged = decorated.join(status_df, on="matchName", how="left")
    enriched = merged.with_columns(
        pl.col("State conservation status rank").is_not_null().alias("has_nynhp_status")
    )
    return enriched.drop("matchName")


def run_pipeline(verbose: bool = True) -> PipelineResult:
    bounding_box, polygon_geojson = fetch_marbletown_boundary()
    geometry_wkt = geojson_polygon_to_wkt(polygon_geojson)
    bbox_wkt = bounding_box_to_wkt(bounding_box)

    occurrences = fetch_gbif_occurrences(geometry_wkt)
    occurrence_df = build_occurrence_frame(occurrences)
    status_df = load_status_reference()
    combined_df = tag_occurrences_with_status(occurrence_df, status_df)

    with_status_count = (
        int(combined_df.select(pl.col("has_nynhp_status").sum()).to_series(0).item())
        if combined_df.height
        else 0
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined_df.write_parquet(OUTPUT_PATH)

    if verbose:
        rounded_bbox = tuple(round(value, 6) for value in bounding_box)
        print("Marbletown administrative boundary (OSM):")
        print(f"  Bounding box (south, north, west, east): {rounded_bbox}")
        print(f"  Boundary polygon type: {polygon_geojson.get('type')}")
        print()
        print("GBIF query geometry (WKT, administrative polygon):")
        print(f"  {geometry_wkt}")
        print()
        print("Bounding box WKT (for reference):")
        print(f"  {bbox_wkt}")
        print(f"\nGBIF occurrences retrieved: {len(occurrences)}")
        print(f"Occurrences with NYS conservation status: {with_status_count}")
        print(f"\nDetailed results written to: {OUTPUT_PATH.resolve()}")
        print(f"Total rows saved: {combined_df.height}")

    return PipelineResult(
        bounding_box=tuple(bounding_box),
        geometry_wkt=geometry_wkt,
        bbox_wkt=bbox_wkt,
        occurrences_total=len(occurrences),
        with_status_count=with_status_count,
        dataframe=combined_df,
        output_path=OUTPUT_PATH,
    )


def ensure_dataset(force: bool = False, verbose: bool = False) -> Path:
    if OUTPUT_PATH.exists() and not force:
        return OUTPUT_PATH

    try:
        run_pipeline(verbose=verbose)
    except Exception as exc:  # pylint: disable=broad-except
        if OUTPUT_PATH.exists():
            if verbose:
                print(f"Warning: GBIF/Nominatim request failed ({exc}); using existing dataset.")
            return OUTPUT_PATH
        raise

    return OUTPUT_PATH


__all__ = [
    "PipelineResult",
    "OUTPUT_PATH",
    "ensure_dataset",
    "run_pipeline",
    "fetch_marbletown_boundary",
    "bounding_box_to_wkt",
    "geojson_polygon_to_wkt",
]
