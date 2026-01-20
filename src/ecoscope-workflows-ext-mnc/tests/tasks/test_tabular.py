import pytest
import pandas as pd
import numpy as np
import warnings
from ecoscope_workflows_ext_mnc.tasks._tabular import (
    add_totals_row,
    map_column_values,
    replace_missing_with_label,
    convert_to_int,
    to_sentence_case,
    create_bins,
    bin_columns,
    order_bins,
    categorize_bins,
    drop_null_values,
    remove_substring,
    remove_brackets_from_column,
    pivot_df,
    clean_dataframe_index,
    map_name_values,
    filter_non_empty_values,
    explode_multiple_columns,
    round_values,
)


class TestAddTotalsRow:
    """Test suite for add_totals_row function"""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({"category": ["A", "B", "C"], "value1": [10, 20, 30], "value2": [5, 15, 25]})

    def test_add_totals_basic(self, sample_df):
        """Test basic totals row addition"""
        result = add_totals_row(sample_df)
        assert len(result) == 4
        assert result.iloc[-1]["value1"] == 60
        assert result.iloc[-1]["value2"] == 45

    def test_add_totals_with_single_label_col(self, sample_df):
        """Test totals with single label column"""
        result = add_totals_row(sample_df, label_col="category", label="Grand Total")
        assert result.iloc[-1]["category"] == "Grand Total"
        assert result.iloc[-1]["value1"] == 60

    def test_add_totals_with_multiple_label_cols(self, sample_df):
        """Test totals with multiple label columns"""
        df = sample_df.copy()
        df["type"] = ["X", "Y", "Z"]
        result = add_totals_row(df, label_col=["category", "type"], label="Total")
        assert result.iloc[-1]["category"] == "Total"
        assert result.iloc[-1]["type"] == "Total"

    def test_add_totals_nonexistent_label_col(self, sample_df):
        """Test with non-existent label column"""
        result = add_totals_row(sample_df, label_col="nonexistent")
        assert len(result) == 4
        assert result.iloc[-1]["value1"] == 60


class TestMapColumnValues:
    """Test suite for map_column_values function"""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({"status": ["active", "inactive", "active"], "priority": ["high", "low", "medium"]})

    def test_map_single_column(self, sample_df):
        """Test mapping values in single column"""
        value_map = {"active": "Active", "inactive": "Inactive"}
        result = map_column_values(sample_df, ["status"], value_map)
        assert result["status"].tolist() == ["Active", "Inactive", "Active"]

    def test_map_multiple_columns(self, sample_df):
        """Test mapping values in multiple columns"""
        value_map = {"high": "High", "low": "Low", "medium": "Medium"}
        result = map_column_values(sample_df, ["priority"], value_map)
        assert "High" in result["priority"].values

    def test_map_inplace_false(self, sample_df):
        """Test that original df is not modified when inplace=False"""
        value_map = {"active": "ACTIVE"}
        result = map_column_values(sample_df, ["status"], value_map, inplace=False)
        assert sample_df["status"].iloc[0] == "active"
        assert result["status"].iloc[0] == "ACTIVE"

    def test_map_nonexistent_column_warning(self, sample_df):
        """Test warning for non-existent column"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            map_column_values(sample_df, ["nonexistent"], {"a": "b"})
            assert len(w) == 1
            assert "not found in DataFrame" in str(w[0].message)


class TestReplaceMissingWithLabel:
    """Test suite for replace_missing_with_label function"""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {"col1": [1, np.nan, 3, None], "col2": ["valid", "None", "nan", ""], "col3": ["data", "  ", "NaN", "value"]}
        )

    def test_replace_nan_values(self, sample_df):
        """Test replacing NaN values"""
        result = replace_missing_with_label(sample_df, "col1", "Missing")
        assert result["col1"].iloc[1] == "Missing"
        assert result["col1"].iloc[3] == "Missing"

    def test_replace_string_none(self, sample_df):
        """Test replacing string 'None'"""
        result = replace_missing_with_label(sample_df, "col2", "Unknown")
        assert result["col2"].iloc[1] == "Unknown"

    def test_replace_empty_strings(self, sample_df):
        """Test replacing empty strings"""
        result = replace_missing_with_label(sample_df, "col2", "N/A")
        assert result["col2"].iloc[3] == "N/A"

    def test_replace_multiple_columns(self, sample_df):
        """Test replacing in multiple columns"""
        result = replace_missing_with_label(sample_df, ["col2", "col3"], "Missing")
        assert result["col2"].iloc[2] == "Missing"
        assert result["col3"].iloc[2] == "Missing"

    def test_column_not_found_error(self, sample_df):
        """Test error when column doesn't exist"""
        with pytest.raises(ValueError, match="not found"):
            replace_missing_with_label(sample_df, "nonexistent", "Missing")


class TestConvertToInt:
    """Test suite for convert_to_int function"""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({"col1": ["1", "2", "3"], "col2": ["4.5", "5.8", "6.2"], "col3": ["7", "invalid", "9"]})

    def test_convert_string_to_int(self, sample_df):
        """Test converting string column to int"""
        result = convert_to_int(sample_df, "col1")
        assert result["col1"].dtype == np.int64
        assert result["col1"].tolist() == [1, 2, 3]

    def test_convert_float_string_with_coerce(self, sample_df):
        """Test converting float strings with coerce"""
        result = convert_to_int(sample_df, "col2", errors="coerce", fill_value=0)
        assert result["col2"].dtype == np.int64
        assert result["col2"].tolist() == [4, 5, 6]

    def test_convert_with_invalid_values(self, sample_df):
        """Test converting with invalid values using coerce"""
        result = convert_to_int(sample_df, "col3", errors="coerce", fill_value=0)
        assert result["col3"].iloc[1] == 0

    def test_convert_multiple_columns(self, sample_df):
        """Test converting multiple columns"""
        result = convert_to_int(sample_df, ["col1", "col2"])
        assert result["col1"].dtype == np.int64
        assert result["col2"].dtype == np.int64

    def test_convert_nonexistent_column(self, sample_df, capsys):
        """Test warning for non-existent column"""
        convert_to_int(sample_df, "nonexistent")
        captured = capsys.readouterr()
        assert "not found in DataFrame" in captured.out


class TestToSentenceCase:
    """Test suite for to_sentence_case function"""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {"name": ["JOHN DOE", "jane smith", "BOB JONES"], "city": ["NEW YORK", "los angeles", "CHICAGO"]}
        )

    def test_convert_single_column(self, sample_df):
        """Test converting single column to sentence case"""
        result = to_sentence_case(sample_df, "name")
        assert result["name"].iloc[0] == "John doe"
        assert result["name"].iloc[1] == "Jane smith"

    def test_convert_multiple_columns(self, sample_df):
        """Test converting multiple columns"""
        result = to_sentence_case(sample_df, ["name", "city"])
        assert result["city"].iloc[0] == "New york"
        assert result["city"].iloc[2] == "Chicago"

    def test_empty_dataframe_error(self):
        """Test error with empty dataframe"""
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError, match="DataFrame is empty"):
            to_sentence_case(empty_df, "col")

    def test_nonexistent_column_error(self, sample_df):
        """Test error with non-existent column"""
        with pytest.raises(ValueError, match="not found in DataFrame"):
            to_sentence_case(sample_df, "nonexistent")


class TestCreateBins:
    """Test suite for create_bins function"""

    def test_basic_binning(self):
        """Test basic binning functionality"""
        series = pd.Series([1, 5, 10, 15, 20])
        result = create_bins(series, bins=4)
        assert result.notna().sum() == 5
        assert all(isinstance(x, str) or x is None for x in result)

    def test_negative_values_excluded(self):
        """Test that negative values are excluded"""
        series = pd.Series([-5, 0, 5, 10])
        result = create_bins(series, bins=3)
        assert result.iloc[0] is None
        assert result.iloc[1] is None
        assert result.iloc[2] is not None

    def test_empty_series(self):
        """Test with empty series"""
        series = pd.Series([])
        result = create_bins(series, bins=5)
        assert len(result) == 0

    def test_all_same_values(self):
        """Test with all same values"""
        series = pd.Series([5, 5, 5, 5])
        result = create_bins(series, bins=3)
        assert result.notna().sum() == 4


class TestBinColumns:
    """Test suite for bin_columns function"""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({"age": [25, 35, 45, 55, 65], "salary": [30000, 50000, 70000, 90000, 110000]})

    def test_bin_single_column(self, sample_df):
        """Test binning single column"""
        result = bin_columns(sample_df, "age", bins=3)
        assert "age bins" in result.columns
        assert result["age bins"].notna().sum() > 0

    def test_bin_multiple_columns(self, sample_df):
        """Test binning multiple columns"""
        result = bin_columns(sample_df, ["age", "salary"], bins=3)
        assert "age bins" in result.columns
        assert "salary bins" in result.columns

    def test_custom_suffix(self, sample_df):
        """Test custom suffix"""
        result = bin_columns(sample_df, "age", bins=3, suffix="_categories")
        assert "age_categories" in result.columns

    def test_inplace_false(self, sample_df):
        """Test that original df is preserved when inplace=False"""
        result = bin_columns(sample_df, "age", bins=3, inplace=False)
        assert "age bins" not in sample_df.columns
        assert "age bins" in result.columns


class TestOrderBins:
    """Test suite for order_bins function"""

    def test_order_bins_basic(self):
        """Test ordering bins"""
        df = pd.DataFrame({"bins": ["10–20", "1–10", "20–30", "1–10"]})
        result = order_bins(df, "bins")
        categories = result["bins"].cat.categories.tolist()
        assert categories[0] == "1–10"
        assert categories[-1] == "20–30"

    def test_order_bins_with_nulls(self):
        """Test ordering bins with null values"""
        df = pd.DataFrame({"bins": ["10–20", None, "1–10", "20–30"]})
        result = order_bins(df, "bins")
        assert result["bins"].cat.ordered


class TestCategorizeBins:
    """Test suite for categorize_bins function"""

    def test_categorize_bins_basic(self):
        """Test basic bin categorization"""
        df = pd.DataFrame({"bins": ["10–20", "1–10", "20–30", "5–15"]})
        result = categorize_bins(df, "bins")
        assert result["bins"].cat.ordered
        assert len(result) == 4

    def test_categorize_bins_filters_nonpositive(self):
        """Test filtering non-positive bins"""
        df = pd.DataFrame({"bins": ["10–20", "0–5", "-5–0", "1–10"], "value": [1, 2, 3, 4]})
        result = categorize_bins(df, "bins")
        assert len(result) < len(df)

    def test_categorize_bins_with_intervals(self):
        """Test with interval notation"""
        df = pd.DataFrame({"bins": ["(0, 10]", "(10, 20]", "(20, 30]"]})
        result = categorize_bins(df, "bins")
        assert result["bins"].cat.ordered


class TestDropNullValues:
    """Test suite for drop_null_values function"""

    def test_drop_nulls_basic(self):
        """Test dropping null values"""
        df = pd.DataFrame({"col1": [1, 2, None, 4], "col2": ["a", "b", "c", "d"]})
        result = drop_null_values(df, "col1")
        assert len(result) == 3
        assert result["col1"].isna().sum() == 0

    def test_drop_nulls_column_not_found(self):
        """Test error when column not found"""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        with pytest.raises(ValueError, match="not found in dataframe"):
            drop_null_values(df, "nonexistent")


class TestRemoveSubstring:
    """Test suite for remove_substring function"""

    def test_remove_substring_basic(self):
        """Test basic substring removal"""
        df = pd.DataFrame({"text": ["Hello World", "World Hello", "Test World"]})
        result = remove_substring(df, "text", "World")
        assert "World" not in result["text"].iloc[0]
        assert result["text"].iloc[0] == "Hello"

    def test_remove_substring_case_insensitive(self):
        """Test case-insensitive removal"""
        df = pd.DataFrame({"text": ["Hello WORLD", "world hello"]})
        result = remove_substring(df, "text", "world")
        assert result["text"].iloc[0] == "Hello"

    def test_remove_substring_empty_df(self):
        """Test error with empty dataframe"""
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="DataFrame is empty"):
            remove_substring(df, "text", "value")

    def test_remove_substring_column_not_found(self):
        """Test error when column not found"""
        df = pd.DataFrame({"col1": ["text"]})
        with pytest.raises(ValueError, match="not found in DataFrame"):
            remove_substring(df, "nonexistent", "value")


class TestRemoveBracketsFromColumn:
    """Test suite for remove_brackets_from_column function"""

    def test_remove_brackets_basic(self):
        """Test removing brackets from list values"""
        df = pd.DataFrame({"col1": [["value1"], ["value2"], ["value3"]]})
        result = remove_brackets_from_column(df, ["col1"])
        assert result["col1"].tolist() == ["value1", "value2", "value3"]

    def test_remove_brackets_empty_list(self):
        """Test with empty lists"""
        df = pd.DataFrame({"col1": [["value1"], [], ["value3"]]})
        result = remove_brackets_from_column(df, ["col1"])
        assert result["col1"].iloc[1] is None

    def test_remove_brackets_multiple_columns(self):
        """Test with multiple columns"""
        df = pd.DataFrame({"col1": [["a"], ["b"]], "col2": [["c"], ["d"]]})
        result = remove_brackets_from_column(df, ["col1", "col2"])
        assert result["col1"].iloc[0] == "a"
        assert result["col2"].iloc[0] == "c"


class TestPivotDf:
    """Test suite for pivot_df function"""

    def test_pivot_basic(self):
        """Test basic pivot operation"""
        df = pd.DataFrame(
            {"date": ["2024-01", "2024-01", "2024-02"], "category": ["A", "B", "A"], "value": [10, 20, 30]}
        )
        result = pivot_df(df, "date", "category", "value")
        assert "A" in result.columns
        assert "B" in result.columns

    def test_pivot_no_reset_index(self):
        """Test pivot without resetting index"""
        df = pd.DataFrame({"date": ["2024-01", "2024-02"], "category": ["A", "A"], "value": [10, 20]})
        result = pivot_df(df, "date", "category", "value", reset_idx=False)
        assert isinstance(result.index, pd.Index)


class TestCleanDataframeIndex:
    """Test suite for clean_dataframe_index function"""

    def test_reset_index_basic(self):
        """Test basic index reset"""
        df = pd.DataFrame({"col1": [1, 2, 3]}, index=[10, 20, 30])
        result = clean_dataframe_index(df, reset_index=True, drop_index=True)
        assert result.index.tolist() == [0, 1, 2]

    def test_rename_unnamed_columns(self):
        """Test renaming unnamed columns"""
        df = pd.DataFrame([[1, 2, 3]], columns=["Unnamed: 0", "col1", "Unnamed: 1"])
        result = clean_dataframe_index(df, reset_index=False, rename_unnamed=True)
        assert "idx" in result.columns
        assert "Unnamed: 0" not in result.columns

    def test_custom_unnamed_col_name(self):
        """Test custom name for unnamed columns"""
        df = pd.DataFrame([[1, 2]], columns=["Unnamed: 0", "col1"])
        result = clean_dataframe_index(df, rename_unnamed=True, unnamed_col_name="index_col")
        assert "index_col" in result.columns


class TestMapNameValues:
    """Test suite for map_name_values function"""

    def test_map_name_basic(self):
        """Test basic name mapping"""
        df = pd.DataFrame({"name": ["john_doe", "jane_smith", "bob_jones"]})
        result = map_name_values(df, "name")
        assert result["name"].iloc[0] == "John Doe"
        assert result["name"].iloc[1] == "Jane Smith"

    def test_map_name_column_not_found(self):
        """Test error when column not found"""
        df = pd.DataFrame({"col1": ["value"]})
        with pytest.raises(ValueError, match="not found in DataFrame"):
            map_name_values(df, "nonexistent")


class TestFilterNonEmptyValues:
    """Test suite for filter_non_empty_values function"""

    def test_filter_lists(self):
        """Test filtering with list values"""
        df = pd.DataFrame({"id": [1, 2, 3, 4], "items": [["a", "b"], [], ["c"], None]})
        result = filter_non_empty_values(df, "items")
        assert len(result) == 2
        assert result["id"].tolist() == [1, 3]

    def test_filter_strings(self):
        """Test filtering with string values"""
        df = pd.DataFrame({"id": [1, 2, 3], "text": ["hello", "", "world"]})
        result = filter_non_empty_values(df, "text")
        assert len(result) == 2

    def test_filter_column_not_found(self):
        """Test error when column not found"""
        df = pd.DataFrame({"col1": [1, 2]})
        with pytest.raises(ValueError, match="not found in DataFrame"):
            filter_non_empty_values(df, "nonexistent")


class TestExplodeMultipleColumns:
    """Test suite for explode_multiple_columns function"""

    def test_explode_single_column(self):
        """Test exploding single column"""
        df = pd.DataFrame({"id": [1, 2], "items": [["a", "b"], ["c"]]})
        result = explode_multiple_columns(df, "items")
        assert len(result) == 3
        assert result["items"].tolist() == ["a", "b", "c"]

    def test_explode_multiple_columns(self):
        """Test exploding multiple columns sequentially"""
        df = pd.DataFrame({"id": [1, 2], "items": [["a", "b"], ["c"]], "values": [[1, 2], [3]]})
        result = explode_multiple_columns(df, ["items", "values"])
        # Sequential explosion creates Cartesian product:
        # Row 1: items=['a','b'], values=[1,2] -> explode items: 2 rows -> explode values: 4 rows
        # Row 2: items=['c'], values=[3] -> explode items: 1 row -> explode values: 1 row
        # Total: 5 rows
        assert len(result) == 5
        assert "items" in result.columns
        assert "values" in result.columns

        # Verify the Cartesian product behavior
        # For id=1, we get: (a,1), (a,2), (b,1), (b,2)
        id_1_rows = result[result["id"] == 1]
        assert len(id_1_rows) == 4
        assert set(id_1_rows["items"].values) == {"a", "b"}
        assert set(id_1_rows["values"].values) == {1, 2}

    def test_explode_aligned_columns(self):
        """Test exploding with aligned list lengths"""
        df = pd.DataFrame({"id": [1, 2], "items": [["a", "b"], ["c"]], "counts": [[10, 20], [30]]})
        result = explode_multiple_columns(df, ["items", "counts"])
        # When lists are aligned in length, sequential explosion still creates multiplication
        assert len(result) == 5  # (2*2) + (1*1) = 5

    def test_explode_string_lists(self):
        """Test exploding string representations of lists"""
        df = pd.DataFrame({"id": [1, 2], "items": ["['a', 'b']", "['c']"]})
        result = explode_multiple_columns(df, "items")
        assert len(result) > len(df)

    def test_explode_column_not_found(self):
        """Test error when column not found"""
        df = pd.DataFrame({"col1": [1, 2]})
        with pytest.raises(ValueError, match="not found in DataFrame"):
            explode_multiple_columns(df, "nonexistent")


class TestRoundValues:
    """Test suite for round_values function"""

    def test_round_basic(self):
        """Test basic rounding"""
        df = pd.DataFrame({"value": [1.234, 5.678, 9.999]})
        result = round_values(df, "value", 2)
        assert result["value"].tolist() == [1.23, 5.68, 10.0]

    def test_round_to_zero_decimals(self):
        """Test rounding to zero decimals"""
        df = pd.DataFrame({"value": [1.5, 2.8, 3.2]})
        result = round_values(df, "value", 0)
        assert result["value"].tolist() == [2.0, 3.0, 3.0]

    def test_round_empty_dataframe(self):
        """Test error with empty dataframe"""
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="DataFrame is empty"):
            round_values(df, "value", 2)

    def test_round_column_not_found(self):
        """Test error when column not found"""
        df = pd.DataFrame({"col1": [1.5]})
        with pytest.raises(ValueError, match="does not exist"):
            round_values(df, "nonexistent", 2)

    def test_round_non_numeric_column(self):
        """Test error with non-numeric column"""
        df = pd.DataFrame({"text": ["a", "b", "c"]})
        with pytest.raises(TypeError, match="must be numeric"):
            round_values(df, "text", 2)

    def test_round_invalid_decimals(self):
        """Test error with invalid decimals parameter"""
        df = pd.DataFrame({"value": [1.5]})
        with pytest.raises(TypeError, match="must be a numeric value"):
            round_values(df, "value", "invalid")
