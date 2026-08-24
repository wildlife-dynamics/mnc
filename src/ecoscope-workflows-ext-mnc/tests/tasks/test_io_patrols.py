from unittest.mock import MagicMock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import LineString, Polygon

from ecoscope_workflows_ext_mnc.tasks.io._patrols import (
    compute_patrol_occupancy,
    create_patrol_coverage_grid,
    get_patrol_values,
)

UTM36S = "EPSG:32736"

# A point close to the equator (within UTM zone 36S, southern-hemisphere false
# northing of 10_000_000) so that reprojection to the grid's fixed EPSG:3857
# CRS introduces negligible Web Mercator distortion.
BX, BY = 500_000, 9_699_750


def make_traj_gdf(rows):
    return gpd.GeoDataFrame(
        {
            "patrol_id": [r[0] for r in rows],
            "timespan_seconds": [r[1] for r in rows],
            "dist_meters": [r[2] for r in rows],
        },
        geometry=[r[3] for r in rows],
        crs=UTM36S,
    )


class TestCreatePatrolCoverageGrid:
    """Test cases for create_patrol_coverage_grid."""

    @pytest.fixture
    def single_traj(self):
        # A 1000m-long segment straddling two 500m grid cells.
        line = LineString([(BX, BY + 250), (BX + 1000, BY + 250)])
        return make_traj_gdf([("p1", 3600.0, 1000.0, line)])

    def test_raises_on_empty_trajs(self):
        empty = gpd.GeoDataFrame({"patrol_id": [], "timespan_seconds": [], "dist_meters": []}, geometry=[], crs=UTM36S)
        with pytest.raises(ValueError, match="empty"):
            create_patrol_coverage_grid(empty)

    def test_raises_on_missing_columns(self):
        gdf = gpd.GeoDataFrame({"geometry": [LineString([(0, 0), (1, 1)])]}, crs=UTM36S)
        with pytest.raises(ValueError, match="Missing required columns"):
            create_patrol_coverage_grid(gdf)

    def test_geographic_input_crs_is_still_handled(self):
        # create_meshgrid always builds the grid in a fixed projected CRS
        # (EPSG:3857) regardless of the input CRS, so a geographic-CRS
        # trajectory GeoDataFrame should not raise and still produce a
        # projected grid.
        line = LineString([(36.0, -1.0), (36.01, -1.0)])
        gdf = gpd.GeoDataFrame(
            {"patrol_id": ["p1"], "timespan_seconds": [1.0], "dist_meters": [1.0]},
            geometry=[line],
            crs="EPSG:4326",
        )
        result = create_patrol_coverage_grid(gdf, grid_cell_size=500)
        assert result.crs.is_projected

    def test_returns_grid_with_summary_columns(self, single_traj):
        result = create_patrol_coverage_grid(single_traj, grid_cell_size=500)

        assert isinstance(result, gpd.GeoDataFrame)
        for col in [
            "grid_id",
            "unique_patrol_count",
            "time_spent_seconds",
            "distance_patrolled_meters",
            "time_spent_hours",
            "distance_patrolled_km",
        ]:
            assert col in result.columns

    def test_time_and_distance_conserved_across_cells(self, single_traj):
        result = create_patrol_coverage_grid(single_traj, grid_cell_size=500)

        # Small tolerance for reprojection distortion into the grid's fixed
        # EPSG:3857 CRS.
        assert result["distance_patrolled_meters"].sum() == pytest.approx(1000.0, rel=1e-2)
        assert result["time_spent_seconds"].sum() == pytest.approx(3600.0, rel=1e-2)

    def test_sorted_by_unique_patrol_count_descending(self, single_traj):
        result = create_patrol_coverage_grid(single_traj, grid_cell_size=500)
        counts = result["unique_patrol_count"].tolist()
        assert counts == sorted(counts, reverse=True)

    def test_zero_distance_segments_excluded(self):
        line = LineString([(0, 250), (0, 250)])  # zero-length
        gdf = make_traj_gdf([("p1", 100.0, 0.0, line)])
        result = create_patrol_coverage_grid(gdf, grid_cell_size=500, keep_empty_cells=True)
        assert (result["unique_patrol_count"] == 0).all()

    def test_no_intersection_drops_empty_cells_by_default(self):
        line = LineString([(0, 250), (0, 250)])
        gdf = make_traj_gdf([("p1", 100.0, 0.0, line)])
        result = create_patrol_coverage_grid(gdf, grid_cell_size=500, keep_empty_cells=False)
        assert len(result) == 0

    def test_no_intersection_keeps_empty_cells_when_requested(self):
        line = LineString([(0, 250), (0, 250)])
        gdf = make_traj_gdf([("p1", 100.0, 0.0, line)])
        result = create_patrol_coverage_grid(gdf, grid_cell_size=500, keep_empty_cells=True)
        assert len(result) > 0
        assert (result["time_spent_seconds"] == 0.0).all()

    def test_aoi_restricts_output_to_intersecting_cells(self, single_traj):
        # Covers only the first half of the trajectory's 1000m extent.
        aoi = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(BX, BY), (BX, BY + 500), (BX + 500, BY + 500), (BX + 500, BY)])],
            crs=UTM36S,
        )
        result = create_patrol_coverage_grid(single_traj, aoi=aoi, grid_cell_size=500)
        aoi_union = aoi.to_crs(result.crs).geometry.union_all()
        assert len(result) > 0
        assert all(result.geometry.intersects(aoi_union))

    def test_multiple_patrols_counted_uniquely(self):
        line = LineString([(10, 10), (400, 10)])
        gdf = make_traj_gdf(
            [
                ("p1", 100.0, 390.0, line),
                ("p2", 200.0, 390.0, line),
            ]
        )
        result = create_patrol_coverage_grid(gdf, grid_cell_size=500)
        assert result["unique_patrol_count"].max() == 2


class TestGetPatrolValues:
    """Test cases for get_patrol_values."""

    def test_raises_when_column_missing(self):
        df = pd.DataFrame({"other": [1, 2]})
        client = MagicMock()
        with pytest.raises(ValueError, match="not found"):
            get_patrol_values(df, patrols_column="patrol_id", client=client)

    def test_returns_empty_dataframe_when_no_patrol_ids(self):
        df = pd.DataFrame({"patrol_id": [None, None]})
        client = MagicMock()
        result = get_patrol_values(df, patrols_column="patrol_id", client=client)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_fetches_unique_patrols_successfully(self):
        df = pd.DataFrame({"patrol_id": ["p1", "p2", "p1"]})
        client = MagicMock()
        client._get.side_effect = lambda url: {"id": url.split("/")[-2]}

        result = get_patrol_values(df, patrols_column="patrol_id", client=client)

        assert len(result) == 2
        assert set(result["id"]) == {"p1", "p2"}
        assert client._get.call_count == 2

    def test_empty_response_excluded_from_results(self):
        df = pd.DataFrame({"patrol_id": ["p1", "p2"]})
        client = MagicMock()
        client._get.side_effect = lambda url: {} if "p1" in url else {"id": "p2"}

        result = get_patrol_values(df, patrols_column="patrol_id", client=client)

        assert len(result) == 1
        assert result["id"].iloc[0] == "p2"

    def test_errors_excluded_from_results_and_do_not_raise(self):
        df = pd.DataFrame({"patrol_id": ["p1", "p2"]})
        client = MagicMock()

        def fetch(url):
            if "p1" in url:
                raise RuntimeError("boom")
            return {"id": "p2"}

        client._get.side_effect = fetch

        result = get_patrol_values(df, patrols_column="patrol_id", client=client)

        assert len(result) == 1
        assert result["id"].iloc[0] == "p2"

    def test_all_errors_returns_empty_dataframe(self):
        df = pd.DataFrame({"patrol_id": ["p1"]})
        client = MagicMock()
        client._get.side_effect = RuntimeError("boom")

        result = get_patrol_values(df, patrols_column="patrol_id", client=client)

        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestComputePatrolOccupancy:
    """Test cases for compute_patrol_occupancy."""

    def test_raises_when_crs_missing(self):
        conservancies = gpd.GeoDataFrame({"name": ["A"]}, geometry=[Polygon([(0, 0), (0, 10), (10, 10), (10, 0)])])
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[Polygon([(0, 0), (0, 10), (10, 10), (10, 0)])], crs=UTM36S)
        with pytest.raises(ValueError, match="no CRS"):
            compute_patrol_occupancy(conservancies, coverage)

    def test_raises_when_crs_is_geographic(self):
        conservancies = gpd.GeoDataFrame(
            {"name": ["A"]}, geometry=[Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])], crs="EPSG:4326"
        )
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])], crs="EPSG:4326")
        with pytest.raises(ValueError, match="geographic CRS"):
            compute_patrol_occupancy(conservancies, coverage)

    def test_full_overlap_yields_100_percent(self):
        square = Polygon([(0, 0), (0, 100), (100, 100), (100, 0)])
        conservancies = gpd.GeoDataFrame({"name": ["A"]}, geometry=[square], crs=UTM36S)
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[square], crs=UTM36S)

        result = compute_patrol_occupancy(conservancies, coverage)

        assert result["conservancy_name"].iloc[0] == "A"
        assert result["occupancy_percentage"].iloc[0] == 100.0
        assert result["conservancy_area_sqkm"].iloc[0] == result["patrolled_area_sqkm"].iloc[0]

    def test_no_overlap_yields_zero_percent(self):
        conservancy = Polygon([(0, 0), (0, 100), (100, 100), (100, 0)])
        coverage_geom = Polygon([(1000, 1000), (1000, 1100), (1100, 1100), (1100, 1000)])
        conservancies = gpd.GeoDataFrame({"name": ["A"]}, geometry=[conservancy], crs=UTM36S)
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[coverage_geom], crs=UTM36S)

        result = compute_patrol_occupancy(conservancies, coverage)

        assert result["occupancy_percentage"].iloc[0] == 0.0
        assert result["patrolled_area_sqkm"].iloc[0] == 0.0

    def test_partial_overlap_percentage(self):
        conservancy = Polygon([(0, 0), (0, 100), (100, 100), (100, 0)])  # 100x100
        coverage_geom = Polygon([(0, 0), (0, 100), (50, 100), (50, 0)])  # half of it
        conservancies = gpd.GeoDataFrame({"name": ["A"]}, geometry=[conservancy], crs=UTM36S)
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[coverage_geom], crs=UTM36S)

        result = compute_patrol_occupancy(conservancies, coverage)

        assert result["occupancy_percentage"].iloc[0] == pytest.approx(50.0)

    def test_zero_area_region_is_skipped(self, capsys):
        degenerate = Polygon([(0, 0), (0, 0), (0, 0)])
        real = Polygon([(0, 0), (0, 100), (100, 100), (100, 0)])
        conservancies = gpd.GeoDataFrame({"name": ["Empty", "Real"]}, geometry=[degenerate, real], crs=UTM36S)
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[real], crs=UTM36S)

        result = compute_patrol_occupancy(conservancies, coverage)

        assert list(result["conservancy_name"]) == ["Real"]
        assert "Empty" in capsys.readouterr().out

    def test_multiple_coverage_features_are_unioned(self):
        conservancy = Polygon([(0, 0), (0, 100), (100, 100), (100, 0)])
        left_half = Polygon([(0, 0), (0, 100), (50, 100), (50, 0)])
        right_half = Polygon([(50, 0), (50, 100), (100, 100), (100, 0)])
        conservancies = gpd.GeoDataFrame({"name": ["A"]}, geometry=[conservancy], crs=UTM36S)
        coverage = gpd.GeoDataFrame({"id": [1, 2]}, geometry=[left_half, right_half], crs=UTM36S)

        result = compute_patrol_occupancy(conservancies, coverage)

        assert result["occupancy_percentage"].iloc[0] == pytest.approx(100.0)

    def test_empty_conservancies_returns_empty_dataframe(self):
        conservancies = gpd.GeoDataFrame({"name": []}, geometry=[], crs=UTM36S)
        coverage = gpd.GeoDataFrame({"id": [1]}, geometry=[Polygon([(0, 0), (0, 10), (10, 10), (10, 0)])], crs=UTM36S)

        result = compute_patrol_occupancy(conservancies, coverage)

        assert isinstance(result, pd.DataFrame)
        assert result.empty
