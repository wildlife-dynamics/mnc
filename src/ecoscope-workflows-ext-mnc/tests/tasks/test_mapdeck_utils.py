import pytest
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
import numpy as np

# Assuming your functions are in a module called 'geo_utils'
from ecoscope_workflows_ext_mnc.tasks._mapdeck_utils import create_gdf_from_dict, exclude_geom_outliers

TEST_DATA_DIR = Path(__file__).parent.parent / "data"


class TestCreateGdfFromDict:
    """Test cases for create_gdf_from_dict function."""
    
    @pytest.fixture
    def sample_gdf_dict(self):
        """Create a sample dictionary of GeoDataFrames for testing."""
        gdf1 = gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
        gdf2 = gpd.read_file(TEST_DATA_DIR / "AOIs.gpkg")
        gdf3 = gpd.read_file(TEST_DATA_DIR / "kenya_pa.gpkg")
        
        return {
            "trajectories": gdf1,
            "areas_of_interest": gdf2,
            "protected_areas": gdf3
        }
    
    def test_retrieve_existing_key_exact_match(self, sample_gdf_dict):
        """Test retrieving a GeoDataFrame with exact key match."""
        result = create_gdf_from_dict(sample_gdf_dict, "trajectories")
        
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.equals(sample_gdf_dict["trajectories"])
    
    def test_retrieve_existing_key_case_insensitive(self, sample_gdf_dict):
        """Test retrieving a GeoDataFrame with case-insensitive key match."""
        result = create_gdf_from_dict(sample_gdf_dict, "TRAJECTORIES")
        
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.equals(sample_gdf_dict["trajectories"])
    
    def test_retrieve_existing_key_mixed_case(self, sample_gdf_dict):
        """Test retrieving a GeoDataFrame with mixed case key."""
        result = create_gdf_from_dict(sample_gdf_dict, "Areas_Of_Interest")
        
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)
    
    def test_retrieve_nonexistent_key(self, sample_gdf_dict, caplog):
        """Test retrieving with a non-existent key returns None."""
        result = create_gdf_from_dict(sample_gdf_dict, "nonexistent_key")
        
        assert result is None
        assert "Key 'nonexistent_key' not found" in caplog.text
        assert "Available keys:" in caplog.text
    
    def test_empty_dictionary(self, caplog):
        """Test with an empty dictionary."""
        result = create_gdf_from_dict({}, "any_key")
        
        assert result is None
        assert "not found in gdf_dict" in caplog.text
    
    def test_all_keys_retrievable(self, sample_gdf_dict):
        """Test that all keys in dictionary can be retrieved."""
        for key in sample_gdf_dict.keys():
            result = create_gdf_from_dict(sample_gdf_dict, key)
            assert result is not None
            assert isinstance(result, gpd.GeoDataFrame)


class TestExcludeGeomOutliers:
    """Test cases for exclude_geom_outliers function."""
    
    @pytest.fixture
    def sample_trajectory_gdf(self):
        """Load sample trajectory data."""
        return gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg")
    
    @pytest.fixture
    def sample_gdf_with_outliers(self):
        """Create a GeoDataFrame with known outliers."""
        points = [
            Point(0, 0),
            Point(1, 1),
            Point(2, 2),
            Point(1.5, 1.5),
            Point(100, 100),  # Clear outlier
            Point(1.2, 0.8),
            Point(0.8, 1.2)
        ]
        
        gdf = gpd.GeoDataFrame(
            {"id": range(len(points))},
            geometry=points,
            crs="EPSG:4326"
        )
        return gdf
    
    def test_exclude_outliers_default_threshold(self, sample_gdf_with_outliers):
        """Test outlier exclusion with default z-threshold."""
        result = exclude_geom_outliers(sample_gdf_with_outliers)
        
        assert len(result) < len(sample_gdf_with_outliers)
        assert isinstance(result, gpd.GeoDataFrame)
        # The outlier at (100, 100) should be removed
        assert 100 not in result.geometry.x.values
    
    def test_exclude_outliers_custom_threshold(self, sample_gdf_with_outliers):
        """Test outlier exclusion with custom z-threshold."""
        # Lower threshold = more aggressive outlier removal
        result_strict = exclude_geom_outliers(sample_gdf_with_outliers, z_threshold=2.0)
        result_lenient = exclude_geom_outliers(sample_gdf_with_outliers, z_threshold=5.0)
        
        assert len(result_strict) <= len(result_lenient)
    
    def test_empty_dataframe(self, caplog):
        """Test with an empty GeoDataFrame."""
        empty_gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        result = exclude_geom_outliers(empty_gdf)
        
        assert result.empty
        assert "Input dataframe is empty" in caplog.text
    
    def test_too_few_points(self, caplog):
        """Test with fewer than 4 points."""
        points = [Point(0, 0), Point(1, 1), Point(2, 2)]
        gdf = gpd.GeoDataFrame({"id": range(3)}, geometry=points, crs="EPSG:4326")
        
        result = exclude_geom_outliers(gdf)
        
        assert len(result) == 3
        assert "Too few points" in caplog.text
    
    def test_all_points_at_same_location(self, caplog):
        """Test when all points are at the same location (std=0)."""
        points = [Point(5, 5) for _ in range(10)]
        gdf = gpd.GeoDataFrame({"id": range(10)}, geometry=points, crs="EPSG:4326")
        
        result = exclude_geom_outliers(gdf)
        
        assert len(result) == 10
        assert "All points at same location" in caplog.text
    
    def test_missing_geometry_column(self):
        """Test with DataFrame missing geometry column."""
        df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        
        with pytest.raises(ValueError, match="DataFrame must have a 'geometry' column"):
            exclude_geom_outliers(df)
    
    def test_preserves_original_columns(self, sample_gdf_with_outliers):
        """Test that original columns are preserved."""
        result = exclude_geom_outliers(sample_gdf_with_outliers)
        
        assert "id" in result.columns
        assert "geometry" in result.columns
    
    def test_no_modification_to_input(self, sample_gdf_with_outliers):
        """Test that input DataFrame is not modified."""
        original_len = len(sample_gdf_with_outliers)
        exclude_geom_outliers(sample_gdf_with_outliers)
        
        assert len(sample_gdf_with_outliers) == original_len
    
    def test_real_trajectory_data(self, sample_trajectory_gdf):
        """Test with real trajectory data from file."""
        result = exclude_geom_outliers(sample_trajectory_gdf)
        
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) <= len(sample_trajectory_gdf)
        assert result.crs == sample_trajectory_gdf.crs
    
    def test_logging_outliers_count(self, sample_gdf_with_outliers, caplog):
        """Test that outliers count is logged."""
        exclude_geom_outliers(sample_gdf_with_outliers)
        
        assert "Outliers count:" in caplog.text
    
    def test_extreme_threshold_removes_nothing(self, sample_gdf_with_outliers):
        """Test that very high threshold removes no points."""
        result = exclude_geom_outliers(sample_gdf_with_outliers, z_threshold=100.0)
        
        assert len(result) == len(sample_gdf_with_outliers)
    
    def test_very_low_threshold_removes_many(self, sample_gdf_with_outliers):
        """Test that very low threshold removes many points."""
        result = exclude_geom_outliers(sample_gdf_with_outliers, z_threshold=0.5)
        
        assert len(result) < len(sample_gdf_with_outliers)
    
    def test_coordinates_added_correctly(self, sample_gdf_with_outliers):
        """Test that x and y coordinates are calculated correctly."""
        result = exclude_geom_outliers(sample_gdf_with_outliers)
        
        # Verify that x, y match geometry after filtering
        for idx, row in result.iterrows():
            assert row.geometry.x == row["x"]
            assert row.geometry.y == row["y"]


class TestIntegration:
    """Integration tests combining both functions."""
    
    @pytest.fixture
    def multi_layer_dict(self):
        """Create dictionary with multiple layers."""
        return {
            "trajectories": gpd.read_file(TEST_DATA_DIR / "sample_trajs.gpkg"),
            "counties": gpd.read_file(TEST_DATA_DIR / "kenyan_counties.gpkg"),
            "protected_areas": gpd.read_file(TEST_DATA_DIR / "kenya_pa.gpkg")
        }
    
    def test_retrieve_and_filter_pipeline(self, multi_layer_dict):
        """Test complete pipeline: retrieve then filter outliers."""
        gdf = create_gdf_from_dict(multi_layer_dict, "trajectories")
        assert gdf is not None
        
        filtered_gdf = exclude_geom_outliers(gdf, z_threshold=3.0)
        
        assert isinstance(filtered_gdf, gpd.GeoDataFrame)
        assert len(filtered_gdf) <= len(gdf)
    
    def test_case_insensitive_retrieve_and_filter(self, multi_layer_dict):
        """Test pipeline with case-insensitive key."""
        gdf = create_gdf_from_dict(multi_layer_dict, "COUNTIES")
        assert gdf is not None
        
        filtered_gdf = exclude_geom_outliers(gdf)
        
        assert isinstance(filtered_gdf, gpd.GeoDataFrame)

