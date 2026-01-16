import pandas as pd
import geopandas as gpd
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyGeoDataFrame,AnyDataFrame
from ecoscope_workflows_ext_ecoscope.tasks.analysis._create_meshgrid import create_meshgrid
from ecoscope_workflows_ext_ecoscope.tasks.analysis._time_density import CustomGridCellSize
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerClient
import logging
from pydantic import Field
from typing import Annotated

logger = logging.getLogger(__name__)

@task
def create_patrol_coverage_grid(trajs:AnyGeoDataFrame, grid_cell_size=1000)->AnyGeoDataFrame:
    # Validate required columns
    required_cols = ["timespan_seconds", "dist_meters", "patrol_id", "geometry"]
    missing_cols = [col for col in required_cols if col not in trajs.columns]
    
    if missing_cols:
        raise ValueError(
            f"Missing required columns: {missing_cols}. "
            f"Available columns: {list(trajs.columns)}"
        )
    
    if trajs is None or trajs.empty :
        raise ValueError(f"trajs gdf is empty.")
    
    custom_cell_size_config = CustomGridCellSize(grid_cell_size=grid_cell_size)
    grid_gdf = create_meshgrid(
        aoi=trajs,
        intersecting_only=True,
        auto_scale_or_custom_cell_size=custom_cell_size_config,
    )
    
    grid_gdf["grid_id"] = grid_gdf.index
    grid_crs = grid_gdf.crs
    
    trajectories_for_overlay = trajs.to_crs(grid_crs).copy()
    trajectories_for_overlay["original_seg_id"] = range(len(trajectories_for_overlay))
    trajectories_for_overlay = trajectories_for_overlay[[
        "original_seg_id", 
        "timespan_seconds", 
        "dist_meters", 
        "patrol_id", 
        "geometry"
    ]]
    
    clipped_trajs = gpd.overlay(trajectories_for_overlay, grid_gdf, how="intersection")
    
    if clipped_trajs.empty:
        print("No patrol data intersects with the generated grid.")
        return grid_gdf
    
    clipped_trajs["clipped_dist_meters"] = clipped_trajs.geometry.length
    clipped_trajs["dist_meters"] = clipped_trajs["dist_meters"].replace(0, 1)  # Avoid division by zero
    clipped_trajs["clipped_timespan_seconds"] = (
        clipped_trajs["timespan_seconds"] * 
        (clipped_trajs["clipped_dist_meters"] / clipped_trajs["dist_meters"])
    )
    
    grid_summary = clipped_trajs.groupby("grid_id").agg(
        unique_patrol_count=("patrol_id", "nunique"),
        time_spent_seconds=("clipped_timespan_seconds", "sum"),
        distance_patrolled_meters=("clipped_dist_meters", "sum"),
    ).reset_index()
    
    full_grid_summary = grid_gdf.merge(grid_summary, on="grid_id", how="left").fillna(0)
    full_grid_summary["distance_patrolled_km"] = (
        full_grid_summary["distance_patrolled_meters"] / 1000
    ).round(2)
    full_grid_summary["time_spent_hours"] = (
        full_grid_summary["time_spent_seconds"] / 3600
    ).round(2)
    
    full_grid_summary = full_grid_summary[full_grid_summary["unique_patrol_count"] > 0]
    full_grid_summary = full_grid_summary.sort_values(by="unique_patrol_count", ascending=False)
    print(f"full grid summary columns: {full_grid_summary.columns}")
    return full_grid_summary

@task
def compute_occupancy(
    coverage_grid_gdf: AnyGeoDataFrame, 
    regions_gdf: AnyGeoDataFrame,
    crs: str,
) -> AnyDataFrame:
    
    coverage_grid_projected = coverage_grid_gdf.to_crs(crs)
    regions_projected = regions_gdf.to_crs(crs)
    
    patrol_coverage = coverage_grid_projected.geometry.unary_union
    
    if patrol_coverage.is_empty:
        raise ValueError("Patrol coverage geometry is empty.")
    
    results = []
    for idx, region in regions_projected.iterrows():
        region_area = region.geometry.area
        
        if region_area == 0:
            logger.warning(f"Region '{region['name']}' has zero area, skipping.")
            continue
        
        intersection = region.geometry.intersection(patrol_coverage)
        intersection_area = intersection.area
        
        # Calculate what % of THIS conservancy is covered by patrols
        occupancy_pct = 100 * (intersection_area / region_area)
        
        results.append({
            'conservancy_name': region['name'],
            'conservancy_area_sqm': region_area,
            'patrolled_area_sqm': intersection_area,
            'occupancy_percentage': occupancy_pct
        })
    
    results_df = pd.DataFrame(results)
    results_df = results_df[results_df['occupancy_percentage'] > 0].copy()
    results_df = results_df.sort_values('occupancy_percentage', ascending=False).reset_index(drop=True)
    
    return results_df

@task 
def get_patrol_values(
    events_df: AnyDataFrame, 
    patrols_column: str, 
    client:Annotated[EarthRangerClient, Field(description="EarthRanger client")],
    batch_size: int = 10
) -> AnyDataFrame:
    """
    Fetch patrol details from EarthRanger API for unique patrol IDs in batches.
    
    Args:
        events_df: DataFrame containing patrol IDs
        patrols_column: Name of column containing patrol IDs
        client: EarthRanger client instance (with _get method)
        batch_size: Number of patrols to process per batch (default: 10)
        
    Returns:
        DataFrame with patrol details from API
        
    Example:
        >>> patrol_df = get_patrol_values(
        ...     events_df=my_events, 
        ...     patrols_column='patrol_id',
        ...     client=er_client,
        ...     batch_size=10
        ... )
    """
    # Validate column exists
    if patrols_column not in events_df.columns:
        raise ValueError(f"Column '{patrols_column}' not found in DataFrame")
    
    # Get unique patrol IDs
    patrol_list = events_df[patrols_column].unique().tolist()
    patrol_list = [p for p in patrol_list if pd.notna(p)]
    results = []
    
    # Process in batches
    for i in range(0, len(patrol_list), batch_size):
        batch = patrol_list[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(patrol_list) + batch_size - 1) // batch_size
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} patrols)")
        
        for patrol_id in batch:
            try:
                # Fetch patrol details from API
                patrol_data = client._get(f"/activity/patrols/{patrol_id}/")
                
                if patrol_data:
                    results.append(patrol_data)
                    logger.debug(f"Successfully fetched patrol: {patrol_id}")
                else:
                    logger.warning(f"No data returned for patrol: {patrol_id}")
                    
            except Exception as e:
                logger.error(f"Error fetching patrol {patrol_id}: {e}")
                continue
    
    logger.info(f"Successfully fetched {len(results)} patrol records")
    
    # Convert results to DataFrame
    if results:
        patrols_df = pd.DataFrame(results)
        return patrols_df
    else:
        logger.warning("No patrol data fetched, returning empty DataFrame")
        return None