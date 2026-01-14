
import logging
import numpy as np 
from typing import Dict,Optional
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyGeoDataFrame

logger = logging.getLogger(__name__)
@task
def create_gdf_from_dict(gdf_dict: Dict[str, AnyGeoDataFrame], key: str) -> Optional[AnyGeoDataFrame]:
    """
    Retrieve a GeoDataFrame from a dictionary by key.
    
    Args:
        gdf_dict: Dictionary mapping layer names to GeoDataFrames.
        key: The key name to retrieve.
    
    Returns:
        GeoDataFrame if key exists, None otherwise.
    """
    if key in gdf_dict:
        return gdf_dict[key]
    
    # Try case-insensitive lookup
    for dict_key, gdf in gdf_dict.items():
        if dict_key.lower() == key.lower():
            return gdf
    
    logging.info(f"Key '{key}' not found in gdf_dict. Available keys: {list(gdf_dict.keys())}")
    return None

@task
def exclude_geom_outliers(
    df: AnyGeoDataFrame,
    z_threshold: float = 3.0,
) -> AnyGeoDataFrame:
    
    if df.empty:
        logger.warning("Warning: Input dataframe is empty")
        return df
    
    if len(df) < 4:
        logger.warning(f"Warning: Too few points ({len(df)}) for reliable outlier detection. Returning original data.")
        return df
    
    if "geometry" not in df.columns:
        raise ValueError("DataFrame must have a 'geometry' column")
    df = df.copy()

    df["x"] = df.geometry.x
    df["y"] = df.geometry.y

    centroid_x = df["x"].mean()
    centroid_y = df["y"].mean()

    df["dist_from_center"] = np.sqrt(
        (df["x"] - centroid_x)**2 + (df["y"] - centroid_y)**2
    )
    
    dist_mean = df["dist_from_center"].mean()
    dist_std = df["dist_from_center"].std()
    
    if dist_std == 0:
        logger.warning("Warning: All points at same location (std=0). No outliers removed.")
        return df.copy()
    
    z_scores = (df["dist_from_center"] - dist_mean) / dist_std
    mask = np.abs(z_scores) < z_threshold
    df = df[mask].copy()
    outliers_count = (~mask).sum()
    logger.info(f"Outliers count: {outliers_count}")
    return df