import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from ecoscope_workflows_core.tasks.transformation._mapping import RenameColumn
from ecoscope_workflows_ext_mnc.tasks._transform import transform_columns


class TestTransformColumns:
    """Test suite for transform_columns function"""

    @pytest.fixture
    def sample_df(self):
        """Create a sample DataFrame for testing"""
        return pd.DataFrame({"col_a": [1, 2, 3], "col_b": [4, 5, 6], "col_c": [7, 8, 9], "col_d": [10, 11, 12]})

    @pytest.fixture
    def sample_geodf(self):
        """Create a sample GeoDataFrame for testing"""
        return gpd.GeoDataFrame(
            {"col_a": [1, 2, 3], "col_b": [4, 5, 6], "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)]}
        )

    # Test drop_columns functionality
    def test_drop_single_column(self, sample_df):
        """Test dropping a single column"""
        result = transform_columns(sample_df, drop_columns=["col_a"])
        assert "col_a" not in result.columns
        assert list(result.columns) == ["col_b", "col_c", "col_d"]

    def test_drop_multiple_columns(self, sample_df):
        """Test dropping multiple columns"""
        result = transform_columns(sample_df, drop_columns=["col_a", "col_c"])
        assert "col_a" not in result.columns
        assert "col_c" not in result.columns
        assert list(result.columns) == ["col_b", "col_d"]

    def test_drop_geometry_column_warning(self, sample_geodf, caplog):
        """Test that dropping geometry column raises a warning"""
        transform_columns(sample_geodf, drop_columns=["geometry"])
        assert "'geometry' found in drop_columns" in caplog.text

    def test_drop_columns_empty_list(self, sample_df):
        """Test with empty drop_columns list"""
        result = transform_columns(sample_df, drop_columns=[])
        assert list(result.columns) == list(sample_df.columns)

    def test_drop_columns_none(self, sample_df):
        """Test with None drop_columns"""
        result = transform_columns(sample_df, drop_columns=None)
        assert list(result.columns) == list(sample_df.columns)

    # Test retain_columns functionality
    def test_retain_columns_subset(self, sample_df):
        """Test retaining a subset of columns"""
        result = transform_columns(sample_df, retain_columns=["col_b", "col_d"])
        assert list(result.columns) == ["col_b", "col_d"]

    def test_retain_columns_reorder(self, sample_df):
        """Test that retain_columns preserves the specified order"""
        result = transform_columns(sample_df, retain_columns=["col_d", "col_a", "col_c"])
        assert list(result.columns) == ["col_d", "col_a", "col_c"]

    def test_retain_columns_missing_raises_error(self, sample_df):
        """Test that retaining non-existent columns raises KeyError"""
        with pytest.raises(KeyError, match="not all found in DataFrame"):
            transform_columns(sample_df, retain_columns=["col_a", "col_nonexistent"])

    def test_retain_columns_empty_list(self, sample_df):
        """Test with empty retain_columns list"""
        result = transform_columns(sample_df, retain_columns=[])
        assert list(result.columns) == list(sample_df.columns)

    def test_retain_columns_none(self, sample_df):
        """Test with None retain_columns"""
        result = transform_columns(sample_df, retain_columns=None)
        assert list(result.columns) == list(sample_df.columns)

    # Test rename_columns functionality
    def test_rename_single_column_dict(self, sample_df):
        """Test renaming a single column using dictionary"""
        result = transform_columns(sample_df, rename_columns={"col_a": "new_col_a"})
        assert "new_col_a" in result.columns
        assert "col_a" not in result.columns

    def test_rename_multiple_columns_dict(self, sample_df):
        """Test renaming multiple columns using dictionary"""
        result = transform_columns(sample_df, rename_columns={"col_a": "new_a", "col_c": "new_c"})
        assert "new_a" in result.columns
        assert "new_c" in result.columns
        assert "col_a" not in result.columns
        assert "col_c" not in result.columns

    def test_rename_columns_list_format(self, sample_df):
        """Test renaming columns using list of RenameColumn objects"""
        rename_list = [
            RenameColumn(original_name="col_a", new_name="new_a"),
            RenameColumn(original_name="col_b", new_name="new_b"),
        ]
        result = transform_columns(sample_df, rename_columns=rename_list)
        assert "new_a" in result.columns
        assert "new_b" in result.columns

    def test_rename_geometry_column_warning(self, sample_geodf, caplog):
        """Test that renaming geometry column raises a warning"""
        transform_columns(sample_geodf, rename_columns={"geometry": "geom"})
        assert "'geometry' found in rename_columns" in caplog.text

    def test_rename_missing_column_skip(self, sample_df, caplog):
        """Test skipping rename for missing columns with skip_missing_rename=True"""
        result = transform_columns(sample_df, rename_columns={"col_nonexistent": "new_name"}, skip_missing_rename=True)
        assert "Skipping rename for missing columns" in caplog.text
        assert list(result.columns) == list(sample_df.columns)

    def test_rename_missing_column_error(self, sample_df):
        """Test error raised for missing columns with skip_missing_rename=False"""
        with pytest.raises(KeyError, match="not found in DataFrame"):
            transform_columns(sample_df, rename_columns={"col_nonexistent": "new_name"}, skip_missing_rename=False)

    def test_rename_columns_empty_dict(self, sample_df):
        """Test with empty rename_columns dictionary"""
        result = transform_columns(sample_df, rename_columns={})
        assert list(result.columns) == list(sample_df.columns)

    def test_rename_columns_none(self, sample_df):
        """Test with None rename_columns"""
        result = transform_columns(sample_df, rename_columns=None)
        assert list(result.columns) == list(sample_df.columns)

    # Test required_columns functionality
    def test_required_columns_present(self, sample_df):
        """Test that no error is raised when all required columns are present"""
        result = transform_columns(sample_df, required_columns=["col_a", "col_b"])
        assert "col_a" in result.columns
        assert "col_b" in result.columns

    def test_required_columns_missing_raises_error(self, sample_df):
        """Test that missing required columns raise KeyError"""
        with pytest.raises(KeyError, match="Required columns .* not found"):
            transform_columns(sample_df, required_columns=["col_a", "col_nonexistent"])

    def test_required_columns_empty_list(self, sample_df):
        """Test with empty required_columns list"""
        result = transform_columns(sample_df, required_columns=[])
        assert list(result.columns) == list(sample_df.columns)

    def test_required_columns_none(self, sample_df):
        """Test with None required_columns"""
        result = transform_columns(sample_df, required_columns=None)
        assert list(result.columns) == list(sample_df.columns)

    # Test combined operations
    def test_combined_drop_and_rename(self, sample_df):
        """Test dropping and renaming columns together"""
        result = transform_columns(sample_df, drop_columns=["col_a"], rename_columns={"col_b": "new_b"})
        assert "col_a" not in result.columns
        assert "new_b" in result.columns
        assert "col_b" not in result.columns

    def test_combined_retain_and_rename(self, sample_df):
        """Test retaining and renaming columns together"""
        result = transform_columns(sample_df, retain_columns=["col_a", "col_b"], rename_columns={"col_a": "new_a"})
        assert list(result.columns) == ["new_a", "col_b"]

    def test_combined_all_operations(self, sample_df):
        """Test all operations together in correct order"""
        result = transform_columns(
            sample_df,
            drop_columns=["col_d"],
            retain_columns=["col_a", "col_b", "col_c"],
            rename_columns={"col_a": "new_a", "col_c": "new_c"},
            required_columns=["col_a", "col_b"],
        )
        assert list(result.columns) == ["new_a", "col_b", "new_c"]

    def test_operation_order_drop_before_retain(self, sample_df):
        """Test that drop happens before retain"""
        result = transform_columns(sample_df, drop_columns=["col_d"], retain_columns=["col_a", "col_b"])
        assert list(result.columns) == ["col_a", "col_b"]
        assert "col_d" not in result.columns

    def test_operation_order_rename_after_retain(self, sample_df):
        """Test that rename happens after retain"""
        result = transform_columns(sample_df, retain_columns=["col_b", "col_a"], rename_columns={"col_a": "new_a"})
        assert list(result.columns) == ["col_b", "new_a"]

    # Test edge cases
    def test_empty_dataframe(self):
        """Test with empty DataFrame"""
        empty_df = pd.DataFrame()
        result = transform_columns(empty_df)
        assert result.empty

    def test_dataframe_with_one_column(self):
        """Test with DataFrame containing only one column"""
        df = pd.DataFrame({"col_a": [1, 2, 3]})
        result = transform_columns(df, rename_columns={"col_a": "new_a"})
        assert list(result.columns) == ["new_a"]

    def test_preserve_data_values(self, sample_df):
        """Test that data values are preserved after transformation"""
        result = transform_columns(sample_df, rename_columns={"col_a": "new_a"})
        assert result["new_a"].tolist() == [1, 2, 3]

    def test_preserve_dataframe_type(self, sample_geodf):
        """Test that GeoDataFrame type is preserved"""
        result = transform_columns(sample_geodf, rename_columns={"col_a": "new_a"})
        assert isinstance(result, gpd.GeoDataFrame)

    def test_rename_partial_match_with_skip(self, sample_df):
        """Test renaming with mix of existing and non-existing columns"""
        result = transform_columns(
            sample_df, rename_columns={"col_a": "new_a", "col_nonexistent": "new_none"}, skip_missing_rename=True
        )
        assert "new_a" in result.columns
        assert "new_none" not in result.columns
