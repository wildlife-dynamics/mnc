import pandas as pd
import pytest
import geopandas as gpd
from shapely.geometry import Point
from ecoscope_workflows_core.skip import SKIP_SENTINEL
from ecoscope_workflows_ext_mnc.tasks._merge import merge_dataframes,merge_multiple_df

def test_merge_dataframes_basic_left_merge():
    left = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value_left": ["a", "b", "c"],
        }
    )

    right = pd.DataFrame(
        {
            "id": [2, 3],
            "value_right": ["x", "y"],
        }
    )

    result = merge_dataframes(left, right, on="id")

    expected = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value_left": ["a", "b", "c"],
            "value_right": [None, "x", "y"],
        }
    )

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)



def test_merge_dataframes_inner_join():
    left = pd.DataFrame({"id": [1, 2, 3]})
    right = pd.DataFrame({"id": [2, 3]})

    result = merge_dataframes(left, right, on="id", how="inner")

    expected = pd.DataFrame({"id": [2, 3]})

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_merge_dataframes_preserve_left_index():
    left = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        },
        index=pd.Index([10, 20, 30], name="custom_index"),
    )

    right = pd.DataFrame(
        {
            "id": [2, 3],
            "extra": ["x", "y"],
        }
    )

    result = merge_dataframes(
        left,
        right,
        on="id",
        preserve_left_index=True,
    )

    # Index should be preserved
    assert list(result.index) == [10, 20, 30]
    assert result.index.name == "custom_index"

    expected = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
            "extra": [None, "x", "y"],
        },
        index=pd.Index([10, 20, 30], name="custom_index"),
    )

    pd.testing.assert_frame_equal(result, expected)



def test_merge_dataframes_does_not_preserve_index_by_default():
    left = pd.DataFrame(
        {"id": [1, 2]},
        index=pd.Index([100, 200], name="left_idx"),
    )

    right = pd.DataFrame({"id": [1, 2]})

    result = merge_dataframes(left, right, on="id")

    # Default merge creates a new RangeIndex
    assert isinstance(result.index, pd.RangeIndex)


def test_merge_dataframes_unmatched_keys_preserve_index():
    left = pd.DataFrame(
        {"id": [1, 2]},
        index=[5, 6],
    )

    right = pd.DataFrame(
        {"id": [3], "value": ["x"]},
    )

    result = merge_dataframes(
        left,
        right,
        on="id",
        preserve_left_index=True,
    )

    assert list(result.index) == [5, 6]
    assert result["value"].isna().all()

def test_merge_dataframes_empty_right():
    left = pd.DataFrame({"id": [1, 2]})
    right = pd.DataFrame(columns=["id", "value"])

    result = merge_dataframes(left, right, on="id")

    assert len(result) == 2
    assert "value" in result.columns
    assert result["value"].isna().all()

class TestMergeMultipleDf:
    """Test suite for merge_multiple_df function"""
    
    @pytest.fixture
    def sample_df1(self):
        """Create first sample DataFrame"""
        return pd.DataFrame({
            'id': [1, 2],
            'value': [10, 20]
        })
    
    @pytest.fixture
    def sample_df2(self):
        """Create second sample DataFrame"""
        return pd.DataFrame({
            'id': [3, 4],
            'value': [30, 40]
        })
    
    @pytest.fixture
    def sample_df3(self):
        """Create third sample DataFrame"""
        return pd.DataFrame({
            'id': [5, 6],
            'value': [50, 60]
        })
    
    @pytest.fixture
    def sample_gdf1(self):
        """Create first sample GeoDataFrame"""
        return gpd.GeoDataFrame(
            {'id': [1, 2], 'name': ['A', 'B']},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326"
        )
    
    @pytest.fixture
    def sample_gdf2(self):
        """Create second sample GeoDataFrame"""
        return gpd.GeoDataFrame(
            {'id': [3, 4], 'name': ['C', 'D']},
            geometry=[Point(2, 2), Point(3, 3)],
            crs="EPSG:4326"
        )
    
    # Basic functionality tests
    def test_merge_two_dataframes(self, sample_df1, sample_df2):
        """Test merging two dataframes"""
        result = merge_multiple_df([sample_df1, sample_df2])
        assert len(result) == 4
        assert result['id'].tolist() == [1, 2, 3, 4]
        assert result['value'].tolist() == [10, 20, 30, 40]
    
    def test_merge_three_dataframes(self, sample_df1, sample_df2, sample_df3):
        """Test merging three dataframes"""
        result = merge_multiple_df([sample_df1, sample_df2, sample_df3])
        assert len(result) == 6
        assert result['id'].tolist() == [1, 2, 3, 4, 5, 6]
    
    def test_merge_single_dataframe(self, sample_df1):
        """Test merging a single dataframe"""
        result = merge_multiple_df([sample_df1])
        assert len(result) == len(sample_df1)
        assert result.equals(sample_df1)
    
    # SkipSentinel handling tests
    def test_merge_with_skip_sentinel_at_start(self, sample_df1, sample_df2):
        """Test merging with SkipSentinel at the start of list"""
        result = merge_multiple_df([SKIP_SENTINEL, sample_df1, sample_df2])
        assert len(result) == 4
        assert result['id'].tolist() == [1, 2, 3, 4]
    
    def test_merge_with_skip_sentinel_in_middle(self, sample_df1, sample_df2, sample_df3):
        """Test merging with SkipSentinel in the middle of list"""
        result = merge_multiple_df([sample_df1, SKIP_SENTINEL, sample_df2, sample_df3])
        assert len(result) == 6
        assert result['id'].tolist() == [1, 2, 3, 4, 5, 6]
    
    def test_merge_with_skip_sentinel_at_end(self, sample_df1, sample_df2):
        """Test merging with SkipSentinel at the end of list"""
        result = merge_multiple_df([sample_df1, sample_df2, SKIP_SENTINEL])
        assert len(result) == 4
        assert result['id'].tolist() == [1, 2, 3, 4]
    
    def test_merge_with_multiple_skip_sentinels(self, sample_df1, sample_df2):
        """Test merging with multiple SkipSentinel values"""
        result = merge_multiple_df([
            SKIP_SENTINEL, 
            sample_df1, 
            SKIP_SENTINEL, 
            sample_df2, 
            SKIP_SENTINEL
        ])
        assert len(result) == 4
        assert result['id'].tolist() == [1, 2, 3, 4]
    
    def test_merge_only_skip_sentinels_raises_error(self):
        """Test that merging only SkipSentinel values raises ValueError"""
        with pytest.raises(ValueError, match="No valid dataframes to merge"):
            merge_multiple_df([SKIP_SENTINEL, SKIP_SENTINEL, SKIP_SENTINEL])
    
    def test_merge_single_valid_df_with_skip_sentinels(self, sample_df1):
        """Test merging single valid DataFrame with SkipSentinels"""
        result = merge_multiple_df([SKIP_SENTINEL, sample_df1, SKIP_SENTINEL])
        assert len(result) == len(sample_df1)
        assert result.equals(sample_df1)
    
    # Parameter tests
    def test_merge_with_ignore_index_true(self, sample_df1, sample_df2):
        """Test merge with ignore_index=True (default)"""
        result = merge_multiple_df([sample_df1, sample_df2], ignore_index=True)
        assert result.index.tolist() == [0, 1, 2, 3]
    
    def test_merge_with_ignore_index_false(self, sample_df1, sample_df2):
        """Test merge with ignore_index=False"""
        result = merge_multiple_df([sample_df1, sample_df2], ignore_index=False)
        # Index should preserve original indices
        assert result.index.tolist() == [0, 1, 0, 1]
    
    def test_merge_with_sort_true(self, sample_df1, sample_df2):
        """Test merge with sort=True"""
        result = merge_multiple_df([sample_df1, sample_df2], sort=True)
        assert 'id' in result.columns
        assert 'value' in result.columns
    
    def test_merge_with_sort_false(self, sample_df1, sample_df2):
        """Test merge with sort=False (default)"""
        result = merge_multiple_df([sample_df1, sample_df2], sort=False)
        assert len(result) == 4
    
    # Error handling tests
    def test_merge_empty_list_raises_error(self):
        """Test that empty list raises ValueError"""
        with pytest.raises(ValueError, match="list_df cannot be empty"):
            merge_multiple_df([])
    
    # GeoDataFrame tests
    def test_merge_geodataframes(self, sample_gdf1, sample_gdf2):
        """Test merging GeoDataFrames"""
        result = merge_multiple_df([sample_gdf1, sample_gdf2])
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 4
        assert result.crs == sample_gdf1.crs
    
    def test_merge_geodataframes_with_skip_sentinel(self, sample_gdf1, sample_gdf2):
        """Test merging GeoDataFrames with SkipSentinel"""
        result = merge_multiple_df([sample_gdf1, SKIP_SENTINEL, sample_gdf2])
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 4
    
    # Mixed column tests
    def test_merge_dataframes_different_columns(self):
        """Test merging dataframes with different columns"""
        df1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df2 = pd.DataFrame({'a': [5, 6], 'c': [7, 8]})
        result = merge_multiple_df([df1, df2])
        assert len(result) == 4
        assert set(result.columns) == {'a', 'b', 'c'}
        # Missing values filled with NaN
        assert pd.isna(result.iloc[0]['c'])
        assert pd.isna(result.iloc[2]['b'])
    
    def test_merge_preserves_dtypes(self):
        """Test that merging preserves data types"""
        df1 = pd.DataFrame({'int_col': [1, 2], 'str_col': ['a', 'b']})
        df2 = pd.DataFrame({'int_col': [3, 4], 'str_col': ['c', 'd']})
        result = merge_multiple_df([df1, df2])
        assert result['int_col'].dtype == df1['int_col'].dtype
        assert result['str_col'].dtype == df1['str_col'].dtype
    
    # Real-world scenario tests
    def test_workflow_scenario_all_tasks_succeed(self, sample_df1, sample_df2, sample_df3):
        """Test realistic workflow where all tasks succeed"""
        # Simulating three parallel tasks that all succeed
        task_results = [sample_df1, sample_df2, sample_df3]
        result = merge_multiple_df(task_results)
        assert len(result) == 6
    
    def test_workflow_scenario_some_tasks_skipped(self, sample_df1, sample_df2):
        """Test realistic workflow where some tasks are skipped"""
        # Simulating three parallel tasks where middle one is skipped
        task_results = [sample_df1, SKIP_SENTINEL, sample_df2]
        result = merge_multiple_df(task_results)
        assert len(result) == 4
        assert result['id'].tolist() == [1, 2, 3, 4]
    
    def test_workflow_scenario_first_task_skipped(self, sample_df2, sample_df3):
        """Test realistic workflow where first task is skipped"""
        task_results = [SKIP_SENTINEL, sample_df2, sample_df3]
        result = merge_multiple_df(task_results)
        assert len(result) == 4
        assert result['id'].min() == 3  # First valid id is 3
    
    def test_workflow_scenario_last_task_skipped(self, sample_df1, sample_df2):
        """Test realistic workflow where last task is skipped"""
        task_results = [sample_df1, sample_df2, SKIP_SENTINEL]
        result = merge_multiple_df(task_results)
        assert len(result) == 4
        assert result['id'].max() == 4  # Last valid id is 4
    
    def test_merge_empty_dataframes(self):
        """Test merging empty dataframes"""
        df1 = pd.DataFrame({'a': [], 'b': []})
        df2 = pd.DataFrame({'a': [], 'b': []})
        result = merge_multiple_df([df1, df2])
        assert len(result) == 0
        assert list(result.columns) == ['a', 'b']
    
    def test_merge_with_one_empty_dataframe(self, sample_df1):
        """Test merging with one empty dataframe"""
        df_empty = pd.DataFrame({'id': [], 'value': []})
        result = merge_multiple_df([sample_df1, df_empty])
        assert len(result) == len(sample_df1)
    
    def test_merge_large_number_of_dataframes(self):
        """Test merging many dataframes"""
        dfs = [pd.DataFrame({'id': [i], 'value': [i*10]}) for i in range(10)]
        result = merge_multiple_df(dfs)
        assert len(result) == 10
        assert result['id'].tolist() == list(range(10))
    
    def test_merge_with_skip_sentinels_interspersed(self):
        """Test merging with SkipSentinels interspersed throughout"""
        dfs = []
        for i in range(5):
            if i % 2 == 0:
                dfs.append(pd.DataFrame({'id': [i], 'value': [i*10]}))
            else:
                dfs.append(SKIP_SENTINEL)
        
        result = merge_multiple_df(dfs)
        assert len(result) == 3  # Only indices 0, 2, 4 are valid
        assert result['id'].tolist() == [0, 2, 4]
    
    def test_return_type_is_dataframe(self, sample_df1, sample_df2):
        """Test that return type is DataFrame"""
        result = merge_multiple_df([sample_df1, sample_df2])
        assert isinstance(result, pd.DataFrame)
    
    def test_return_type_is_geodataframe(self, sample_gdf1, sample_gdf2):
        """Test that return type is GeoDataFrame when inputs are GeoDataFrames"""
        result = merge_multiple_df([sample_gdf1, sample_gdf2])
        assert isinstance(result, gpd.GeoDataFrame)