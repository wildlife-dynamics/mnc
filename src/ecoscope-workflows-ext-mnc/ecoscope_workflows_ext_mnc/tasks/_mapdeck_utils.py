import logging
import numpy as np
from typing import Dict, Optional
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

    Examples:
        >>> gdf_dict = {"layer1": gdf1, "layer2": gdf2}
        >>> result = create_gdf_from_dict(gdf_dict, "layer1")
        >>> result = create_gdf_from_dict(gdf_dict, "LAYER1")  # Case-insensitive
    """
    if key in gdf_dict:
        return gdf_dict[key]

    # Try case-insensitive lookup
    for dict_key, gdf in gdf_dict.items():
        if dict_key.lower() == key.lower():
            return gdf

    # Use logger instead of logging to ensure proper capture
    logger.info(f"Key '{key}' not found in gdf_dict. Available keys: {list(gdf_dict.keys())}")
    return None


@task
def exclude_geom_outliers(
    df: AnyGeoDataFrame,
    z_threshold: float = 3.0,
) -> AnyGeoDataFrame:
    """
    Exclude geometric outliers from a GeoDataFrame based on distance from centroid.

    This function identifies and removes outlier points based on their distance from the
    centroid using z-score analysis. Points with a z-score exceeding the threshold are
    considered outliers and removed.

    Note: This function only works with Point geometries. If your GeoDataFrame contains
    LineStrings, Polygons, or other geometry types, convert them to Points (e.g., using
    centroids) before calling this function.

    Args:
        df: GeoDataFrame with Point geometries
        z_threshold: Z-score threshold for outlier detection. Higher values are more
                    lenient (default: 3.0, which keeps ~99.7% of normally distributed data)

    Returns:
        Filtered GeoDataFrame with outliers removed. Temporary calculation columns
        (x, y, dist_from_center) are not included in the output.

    Raises:
        ValueError: If DataFrame doesn't have a geometry column or contains non-Point geometries

    Examples:
        >>> # With Point geometries
        >>> filtered_gdf = exclude_geom_outliers(point_gdf, z_threshold=3.0)

        >>> # Convert LineStrings to Points first
        >>> point_gdf = line_gdf.copy()
        >>> point_gdf['geometry'] = point_gdf.geometry.centroid
        >>> filtered_gdf = exclude_geom_outliers(point_gdf)
    """

    if df.empty:
        logger.warning("Warning: Input dataframe is empty")
        return df

    if len(df) < 4:
        logger.warning(f"Warning: Too few points ({len(df)}) for reliable outlier detection. Returning original data.")
        return df

    if "geometry" not in df.columns:
        raise ValueError("DataFrame must have a 'geometry' column")

    # Check if all geometries are Points
    geom_types = df.geometry.geom_type.unique()
    if not all(geom_type == "Point" for geom_type in geom_types):
        raise ValueError(
            f"This function only works with Point geometries. "
            f"Found geometry types: {geom_types.tolist()}. "
            f"Consider converting to points using: df['geometry'] = df.geometry.centroid"
        )

    # Work on a copy to avoid modifying the input
    df = df.copy()

    # Extract x, y coordinates
    df["x"] = df.geometry.x
    df["y"] = df.geometry.y

    # Calculate centroid
    centroid_x = df["x"].mean()
    centroid_y = df["y"].mean()

    # Calculate distance from centroid
    df["dist_from_center"] = np.sqrt((df["x"] - centroid_x) ** 2 + (df["y"] - centroid_y) ** 2)

    # Calculate statistics
    dist_mean = df["dist_from_center"].mean()
    dist_std = df["dist_from_center"].std()

    # Handle case where all points are at the same location
    if dist_std == 0:
        logger.warning("Warning: All points at same location (std=0). No outliers removed.")
        # Return copy without the temporary columns
        return df[df.columns.difference(["x", "y", "dist_from_center"])].copy()

    # Calculate z-scores and filter
    z_scores = (df["dist_from_center"] - dist_mean) / dist_std
    mask = np.abs(z_scores) < z_threshold

    outliers_count = (~mask).sum()
    logger.info(f"Outliers count: {outliers_count}")

    # Filter and remove temporary columns
    df_filtered = df[mask].copy()

    # Drop temporary calculation columns
    columns_to_drop = ["x", "y", "dist_from_center"]
    df_filtered = df_filtered.drop(columns=columns_to_drop, errors="ignore")

    return df_filtered
