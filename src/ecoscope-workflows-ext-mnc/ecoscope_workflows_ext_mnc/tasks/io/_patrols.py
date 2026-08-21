import pandas as pd
import geopandas as gpd
from pydantic import Field
from wt_registry import register
from typing import Annotated, Optional, cast
from ecoscope.platform.annotations import AnyGeoDataFrame, AnyDataFrame
from ecoscope.platform.tasks.analysis._create_meshgrid import create_meshgrid
from ecoscope.platform.tasks.analysis._time_density import CustomGridCellSize
from ecoscope.platform.connections import EarthRangerClient


@register()
def create_patrol_coverage_grid(
    trajs: AnyGeoDataFrame,
    aoi: Optional[AnyGeoDataFrame] = None,
    grid_cell_size: int = 1000,
    keep_empty_cells: bool = False,
) -> AnyGeoDataFrame:
    """
    Summarize patrol trajectory activity into a spatial grid.

    Overlays patrol trajectories onto a regular grid and computes per-cell
    coverage metrics: number of distinct patrols, total time spent, and
    total distance traveled. Time is allocated to each cell proportionally
    to the fraction of a trajectory segment's length that falls within it
    (assumes constant speed within a segment).

    Args:
        trajs:
            GeoDataFrame of patrol trajectory segments. Must contain
            ``patrol_id``, ``timespan_seconds``, ``dist_meters``, and
            ``geometry`` (LineStrings).
        aoi:
            Optional area-of-interest GeoDataFrame. If provided, the grid
            is generated to cover this AOI and cells outside it are
            excluded from the result. If omitted, the grid is generated
            from ``trajs`` directly.
        grid_cell_size:
            Edge length of each square grid cell, in the units of the
            grid's CRS (meters when projected). Defaults to 1000.
        keep_empty_cells:
            If ``True``, cells with no patrol activity are retained in the
            output with zero values for the summary columns. Useful for
            rendering "patrolled vs not patrolled" maps. Defaults to
            ``False``, which drops empty cells.

    Returns:
        GeoDataFrame of grid cells, sorted by ``unique_patrol_count``
        descending. Columns include ``grid_id``, ``geometry``,
        ``unique_patrol_count``, ``time_spent_seconds``,
        ``distance_patrolled_meters``, ``time_spent_hours``,
        ``distance_patrolled_km``.

    Raises:
        ValueError: If ``trajs`` is empty, missing required columns, or
            if the generated grid's CRS is not projected.
    """
    required_cols = ["timespan_seconds", "dist_meters", "patrol_id", "geometry"]
    missing_cols = [c for c in required_cols if c not in trajs.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}. " f"Available columns: {list(trajs.columns)}")
    if trajs is None or trajs.empty:
        raise ValueError("trajs gdf is empty.")

    grid_source = aoi if aoi is not None and not aoi.empty else trajs

    grid_gdf = create_meshgrid(
        aoi=grid_source,
        intersecting_only=True,
        auto_scale_or_custom_cell_size=CustomGridCellSize(grid_cell_size=grid_cell_size),
    )
    grid_gdf["grid_id"] = grid_gdf.index

    if not grid_gdf.crs.is_projected:
        raise ValueError(f"Grid CRS must be projected, got {grid_gdf.crs}")

    trans_gdf = cast(gpd.GeoDataFrame, trajs).to_crs(grid_gdf.crs)[
        ["patrol_id", "timespan_seconds", "dist_meters", "geometry"]
    ]
    trans_gdf = trans_gdf[trans_gdf["dist_meters"] > 0]

    clipped_trajs = gpd.overlay(trans_gdf, cast(gpd.GeoDataFrame, grid_gdf), how="intersection")
    if clipped_trajs.empty:
        print("No patrol data intersects with the generated grid.")
        if keep_empty_cells:
            return grid_gdf.assign(
                unique_patrol_count=0,
                time_spent_seconds=0.0,
                distance_patrolled_meters=0.0,
                time_spent_hours=0.0,
                distance_patrolled_km=0.0,
            )
        return grid_gdf.iloc[0:0]

    clipped_trajs["clipped_dist_meters"] = clipped_trajs.geometry.length
    clipped_trajs["clipped_timespan_seconds"] = (
        clipped_trajs["timespan_seconds"] * clipped_trajs["clipped_dist_meters"] / clipped_trajs["dist_meters"]
    )

    grid_summary = (
        clipped_trajs.groupby("grid_id")
        .agg(
            unique_patrol_count=("patrol_id", "nunique"),
            time_spent_seconds=("clipped_timespan_seconds", "sum"),
            distance_patrolled_meters=("clipped_dist_meters", "sum"),
        )
        .reset_index()
    )

    full = grid_gdf.merge(grid_summary, on="grid_id", how="left")

    if keep_empty_cells:
        full = full.fillna(
            {
                "unique_patrol_count": 0,
                "time_spent_seconds": 0.0,
                "distance_patrolled_meters": 0.0,
            }
        )
    else:
        full = full.dropna(subset=["unique_patrol_count"])

    full = full.assign(
        distance_patrolled_km=lambda d: (d["distance_patrolled_meters"] / 1000).round(2),
        time_spent_hours=lambda d: (d["time_spent_seconds"] / 3600).round(2),
    ).sort_values("unique_patrol_count", ascending=False)

    if aoi is not None and not aoi.empty:
        aoi_union = cast(gpd.GeoDataFrame, aoi).to_crs(grid_gdf.crs).union_all()
        full = full[full.geometry.intersects(aoi_union)]

    return cast(AnyGeoDataFrame, full)


@register()
def get_patrol_values(
    events_df: AnyDataFrame,
    patrols_column: str,
    client: Annotated[EarthRangerClient, Field(description="EarthRanger client")],
    max_workers: int = 10,
) -> AnyDataFrame:
    """Fetch patrol details from EarthRanger API concurrently."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tqdm.auto import tqdm

    if patrols_column not in events_df.columns:
        raise ValueError(f"Column '{patrols_column}' not found in DataFrame")

    patrol_list = [p for p in events_df[patrols_column].unique() if pd.notna(p)]
    if not patrol_list:
        print("No patrol IDs found, returning empty DataFrame")
        return pd.DataFrame()

    print(f"Fetching {len(patrol_list)} patrols with {max_workers} workers")

    def fetch(patrol_id):
        try:
            data = client._get(f"/activity/patrols/{patrol_id}/")
            return patrol_id, data, None
        except Exception as e:
            return patrol_id, None, e

    results = []
    empty = []
    errors = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, pid): pid for pid in patrol_list}
        for future in tqdm(
            as_completed(futures),
            total=len(patrol_list),
            desc="Fetching patrols",
        ):
            patrol_id, data, err = future.result()
            if err is not None:
                errors.append((patrol_id, err))
            elif not data:
                empty.append(patrol_id)
            else:
                results.append(data)

    print(f"Successfully fetched {len(results)} patrol records")
    if empty:
        print(f"{len(empty)} patrols returned no data")
    if errors:
        print(f"{len(errors)} patrols errored (e.g. {errors[0][1]})")

    return pd.DataFrame(results) if results else pd.DataFrame()


@register()
def compute_patrol_occupancy(conservancies: AnyDataFrame, patrol_coverage: AnyDataFrame) -> AnyDataFrame:
    """Compute the percentage of each conservancy covered by patrol activity.

    For every region in ``conservancies``, this calculates the area of overlap
    with the combined patrol coverage and expresses it as a percentage of that
    region's total area. Areas are returned in square kilometres, rounded to
    2 decimal places.

    Parameters
    ----------
    conservancies : geopandas.GeoDataFrame
        Conservancy regions. Must contain a ``name`` column and a geometry
        column. **Must be in a projected CRS whose units are metres** (e.g. a
        UTM zone).
    patrol_coverage : geopandas.GeoDataFrame
        Patrol coverage geometries. All features are unioned into a single
        coverage area before intersection. **Must be in the same projected,
        metre-based CRS as ``conservancies``.**

    Returns
    -------
    pandas.DataFrame
        One row per non-empty conservancy, with columns:
        ``conservancy_name``, ``conservancy_area_sqkm``,
        ``patrolled_area_sqkm``, and ``occupancy_percentage``.
        Regions with zero area are skipped (a message is printed).

    Notes
    -----
    A **projected** CRS is required for the areas to be meaningful. Shapely's
    ``.area`` is purely planar: it operates on the raw coordinate values with
    no knowledge of the CRS. If the geometries are in a geographic CRS such as
    EPSG:4326, the coordinates are longitude/latitude in *degrees*, so ``.area``
    returns square degrees — which vary in real-world size with latitude and
    cannot be converted to km² by dividing by 1e6. Reproject first, for example
    with ``gdf.to_crs(<projected_epsg>)``, before calling this function.

    Both GeoDataFrames must share the same CRS; otherwise the intersection is
    computed on misaligned coordinates and the results are wrong.
    """
    for gdf, label in [(conservancies, "conservancies"), (patrol_coverage, "patrol_coverage")]:
        if gdf.crs is None:
            raise ValueError(f"{label} has no CRS set; reproject to a metre-based projected CRS first.")
        if gdf.crs.is_geographic:
            raise ValueError(
                f"{label} is in a geographic CRS ({gdf.crs.to_string()}); "
                "reproject to a projected CRS in metres (e.g. a UTM zone) first."
            )

    coverage_union = patrol_coverage.geometry.union_all()
    results = []

    for _, region in conservancies.iterrows():
        region_area = region.geometry.area
        if region_area == 0:
            print(f"Region '{region['name']}' has zero area, skipping.")
            continue

        intersection = region.geometry.intersection(coverage_union)
        intersection_area = intersection.area

        # % of THIS conservancy covered by patrols
        occupancy_pct = 100 * (intersection_area / region_area)

        results.append(
            {
                "conservancy_name": region["name"],
                "conservancy_area_sqkm": round(region_area / 1_000_000, 2),
                "patrolled_area_sqkm": round(intersection_area / 1_000_000, 2),
                "occupancy_percentage": round(occupancy_pct, 2),
            }
        )

    return pd.DataFrame(results)
