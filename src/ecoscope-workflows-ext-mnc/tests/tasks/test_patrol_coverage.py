import geopandas as gpd
import pandas as pd
import pytest
from pathlib import Path
from ecoscope_workflows_ext_mnc.tasks._patrol_coverage import create_patrol_coverage_grid, compute_occupancy

TEST_DATA_DIR = Path(__file__).parent.parent / "data"


def test_create_patrol_coverage_grid_basic():
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]
    result = create_patrol_coverage_grid(trajs, grid_cell_size=1000)

    assert isinstance(result, gpd.GeoDataFrame)
    assert not result.empty

    # Expected output columns
    expected_cols = {
        "geometry",
        "grid_id",
        "unique_patrol_count",
        "time_spent_seconds",
        "distance_patrolled_meters",
        "distance_patrolled_km",
        "time_spent_hours",
    }

    assert expected_cols.issubset(result.columns)

    # Only grids with patrols should remain
    assert (result["unique_patrol_count"] > 0).all()


def test_create_patrol_coverage_grid_missing_columns():
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]

    trajs = trajs.drop(columns=["timespan_seconds"])

    with pytest.raises(ValueError, match="Missing required columns"):
        create_patrol_coverage_grid(trajs)


def test_create_patrol_coverage_grid_empty_gdf():
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]

    empty = trajs.iloc[0:0]

    with pytest.raises(ValueError, match="trajs gdf is empty"):
        create_patrol_coverage_grid(empty)


def test_create_patrol_coverage_grid_no_intersections(monkeypatch):
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]

    # Force overlay to return empty
    import geopandas as gpd_module

    def mock_overlay(*args, **kwargs):
        return gpd.GeoDataFrame(columns=args[0].columns, geometry=[])

    monkeypatch.setattr(gpd_module, "overlay", mock_overlay)

    result = create_patrol_coverage_grid(trajs)

    # Should return the grid, not crash
    assert isinstance(result, gpd.GeoDataFrame)
    assert "grid_id" in result.columns


def test_create_patrol_coverage_grid_handles_zero_distance():
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]

    trajs.loc[trajs.index[0], "dist_meters"] = 0

    result = create_patrol_coverage_grid(trajs)

    assert not result["time_spent_seconds"].isna().any()


def test_compute_occupancy_basic():
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]
    coverage_grid = create_patrol_coverage_grid(trajs)

    regions = gpd.read_file(TEST_DATA_DIR / "kenya_pa.gpkg")

    result = compute_occupancy(
        coverage_grid_gdf=coverage_grid,
        regions_gdf=regions,
        crs="EPSG:3857",
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty

    expected_cols = {
        "conservancy_name",
        "conservancy_area_sqm",
        "patrolled_area_sqm",
        "occupancy_percentage",
    }

    assert expected_cols.issubset(result.columns)

    # Percentages must be sane
    assert (result["occupancy_percentage"] > 0).all()
    assert (result["occupancy_percentage"] <= 100).all()


def test_compute_occupancy_empty_patrol_coverage():
    coverage_grid = gpd.GeoDataFrame(
        geometry=[],
        crs="EPSG:4326",
    )

    regions = gpd.read_file(TEST_DATA_DIR / "kenya_pa.gpkg")

    with pytest.raises(ValueError, match="Patrol coverage geometry is empty"):
        compute_occupancy(
            coverage_grid,
            regions,
            crs="EPSG:3857",
        )


def test_compute_occupancy_skips_zero_area_region(caplog):
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]
    coverage_grid = create_patrol_coverage_grid(trajs)

    regions = gpd.read_file(TEST_DATA_DIR / "kenya_pa.gpkg")

    # Force one region to have zero area
    regions.loc[0, "geometry"] = regions.loc[0, "geometry"].boundary

    compute_occupancy(
        coverage_grid,
        regions,
        crs="EPSG:3857",
    )

    assert "zero area" in caplog.text.lower()


def test_compute_occupancy_sorted_descending():
    trajs = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    trajs["patrol_id"] = trajs["id"]
    coverage_grid = create_patrol_coverage_grid(trajs)

    regions = gpd.read_file(TEST_DATA_DIR / "kenya_pa.gpkg")

    result = compute_occupancy(
        coverage_grid,
        regions,
        crs="EPSG:3857",
    )

    assert result["occupancy_percentage"].is_monotonic_decreasing
