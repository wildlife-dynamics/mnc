import geopandas as gpd
import numpy as np


def _metric_crs(gdf: gpd.GeoDataFrame):
    """Local UTM CRS for metre-based distances (UTM 36S across the Mara)."""
    return gdf.crs if gdf.crs is not None and gdf.crs.is_projected else gdf.estimate_utm_crs()


def _find_clusters(xy: np.ndarray, radius_meters: float, min_vehicles: int) -> tuple[list[np.ndarray], np.ndarray]:
    """Indices of the vehicles in each cluster, and the pairwise distance matrix.

    Each vehicle with at least `min_vehicles` vehicles (itself included) within
    `radius_meters` is a centre; centres whose discs share a vehicle are one cluster.

    Equivalent to DBSCAN with eps=`radius_meters` and min_samples=`min_vehicles`, except
    that groups sharing a border vehicle are merged rather than split, since they're the
    same sighting. Each centre's disc keeps its vehicles within 2 x `radius_meters` of one
    another (50 m -> "within 100 m"); merged clusters can be wider.
    """
    dist = np.linalg.norm(xy[:, None, :] - xy[None, :, :], axis=-1)
    within = dist <= radius_meters
    centres = np.flatnonzero(within.sum(axis=1) >= min_vehicles)

    # Union-find over centre vehicles whose discs share any vehicle.
    parent = {c: c for c in centres}

    def find(c):
        while parent[c] != c:
            parent[c] = parent[parent[c]]
            c = parent[c]
        return c

    for i, a in enumerate(centres):
        for b in centres[i + 1 :]:
            if (within[a] & within[b]).any():
                parent[find(a)] = find(b)

    members_by_root: dict = {}
    for c in centres:
        root = find(c)
        members_by_root[root] = members_by_root.get(root, np.zeros(len(xy), dtype=bool)) | within[c]
    return [np.flatnonzero(members) for members in members_by_root.values()], dist
