import pandas as pd
import geopandas as gpd
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyGeoDataFrame,AnyDataFrame
from ecoscope_workflows_ext_ecoscope.tasks.analysis._create_meshgrid import create_meshgrid
from ecoscope_workflows_ext_ecoscope.tasks.analysis._time_density import CustomGridCellSize

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
    
    # Validate patrol coverage
    if patrol_coverage.is_empty:
        raise ValueError("Patrol coverage geometry is empty.")
    
    total_coverage_area = patrol_coverage.area
    if total_coverage_area == 0:
        raise ValueError("Patrol coverage has zero area.")
    
    intersection_areas = regions_projected.intersection(patrol_coverage).area
    occupancy_percentages = 100 * (intersection_areas / total_coverage_area)
    results_df = pd.DataFrame({
        'conservancy_name': regions_projected['name'].values,
        'occupancy_percentage': occupancy_percentages.values
    })
    
    # Filter out conservancies with zero coverage and sort by occupancy
    results_df = results_df[results_df['occupancy_percentage'] > 0].copy()
    results_df = results_df.sort_values('occupancy_percentage', ascending=False).reset_index(drop=True)
    
    return results_df