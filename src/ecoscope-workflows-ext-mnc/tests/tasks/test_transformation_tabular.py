import pandas as pd
import pytest

from ecoscope_workflows_ext_mnc.tasks.transformation._tabular import (
    convert_columns_to_int,
    fill_missing_values,
    remove_brackets_from_column,
    replace_column_values,
)


class TestRemoveBracketsFromColumn:
    """Test cases for remove_brackets_from_column."""

    def test_extracts_first_element_of_list(self):
        df = pd.DataFrame({"tags": [["a", "b"], ["c"], "not_a_list"]})
        result = remove_brackets_from_column(df, columns=["tags"])
        assert result["tags"].tolist() == ["a", "c", "not_a_list"]

    def test_empty_list_becomes_none(self):
        df = pd.DataFrame({"tags": [[], ["x"]]})
        result = remove_brackets_from_column(df, columns=["tags"])
        assert result["tags"].tolist() == [None, "x"]

    def test_accepts_single_column_as_string(self):
        df = pd.DataFrame({"tags": [["a"], ["b"]]})
        result = remove_brackets_from_column(df, columns="tags")
        assert result["tags"].tolist() == ["a", "b"]

    def test_multiple_columns(self):
        df = pd.DataFrame({"a": [["x"]], "b": [["y", "z"]]})
        result = remove_brackets_from_column(df, columns=["a", "b"])
        assert result["a"].tolist() == ["x"]
        assert result["b"].tolist() == ["y"]

    def test_missing_column_is_ignored(self):
        df = pd.DataFrame({"a": [["x"]]})
        result = remove_brackets_from_column(df, columns=["a", "nonexistent"])
        assert list(result.columns) == ["a"]


class TestConvertColumnsToInt:
    """Test cases for convert_columns_to_int."""

    def test_coerce_converts_numeric_strings(self):
        df = pd.DataFrame({"a": ["1", "2", "3"]})
        result = convert_columns_to_int(df, columns="a")
        assert result["a"].tolist() == [1, 2, 3]
        assert result["a"].dtype == int

    def test_coerce_fills_non_numeric_with_fill_value(self):
        df = pd.DataFrame({"a": ["1", "not_a_number", None]})
        result = convert_columns_to_int(df, columns=["a"], fill_value=-1)
        assert result["a"].tolist() == [1, -1, -1]

    def test_raise_raises_on_bad_value(self):
        df = pd.DataFrame({"a": ["1", "oops"]})
        with pytest.raises(ValueError):
            convert_columns_to_int(df, columns=["a"], errors="raise")

    def test_ignore_leaves_column_unchanged_on_failure(self):
        df = pd.DataFrame({"a": ["1", "oops"]})
        result = convert_columns_to_int(df, columns=["a"], errors="ignore")
        assert result["a"].tolist() == ["1", "oops"]

    def test_missing_column_raises_when_errors_is_raise(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(KeyError):
            convert_columns_to_int(df, columns=["missing"], errors="raise")

    def test_missing_column_skipped_when_coerce(self):
        df = pd.DataFrame({"a": [1]})
        result = convert_columns_to_int(df, columns=["missing"], errors="coerce")
        assert "missing" not in result.columns

    def test_invalid_errors_value_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError):
            convert_columns_to_int(df, columns=["a"], errors="bogus")

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"a": ["1", "2"]})
        convert_columns_to_int(df, columns=["a"])
        assert df["a"].tolist() == ["1", "2"]


class TestFillMissingValues:
    """Test cases for fill_missing_values."""

    def test_fills_all_columns_when_subset_omitted(self):
        df = pd.DataFrame({"a": [1, None], "b": [None, 2]})
        result = fill_missing_values(df, value=0)
        assert result["a"].tolist() == [1, 0]
        assert result["b"].tolist() == [0, 2]

    def test_fills_only_named_subset(self):
        df = pd.DataFrame({"a": [1, None], "b": [None, 2]})
        result = fill_missing_values(df, value=0, subset="a")
        assert result["a"].tolist() == [1, 0]
        assert pd.isna(result["b"][0])

    def test_fills_list_subset(self):
        df = pd.DataFrame({"a": [None], "b": [None], "c": [None]})
        result = fill_missing_values(df, value="x", subset=["a", "b"])
        assert result["a"].tolist() == ["x"]
        assert result["b"].tolist() == ["x"]
        assert pd.isna(result["c"][0])

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"a": [1, None]})
        fill_missing_values(df, value=0)
        assert pd.isna(df["a"][1])


class TestReplaceColumnValues:
    """Test cases for replace_column_values."""

    def test_replaces_mapped_values(self):
        df = pd.DataFrame({"status": ["open", "closed", "open"]})
        result = replace_column_values(df, columns=["status"], value_map={"open": "Open", "closed": "Closed"})
        assert result["status"].tolist() == ["Open", "Closed", "Open"]

    def test_leaves_unmapped_values_unchanged(self):
        df = pd.DataFrame({"status": ["open", "pending"]})
        result = replace_column_values(df, columns=["status"], value_map={"open": "Open"})
        assert result["status"].tolist() == ["Open", "pending"]

    def test_accepts_single_column_as_string(self):
        df = pd.DataFrame({"status": ["a"]})
        result = replace_column_values(df, columns="status", value_map={"a": "b"})
        assert result["status"].tolist() == ["b"]

    def test_default_does_not_mutate_input(self):
        df = pd.DataFrame({"status": ["open"]})
        replace_column_values(df, columns=["status"], value_map={"open": "Open"})
        assert df["status"].tolist() == ["open"]

    def test_inplace_mutates_input(self):
        df = pd.DataFrame({"status": ["open"]})
        result = replace_column_values(df, columns=["status"], value_map={"open": "Open"}, inplace=True)
        assert df["status"].tolist() == ["Open"]
        assert result is df

    def test_missing_column_raise(self):
        df = pd.DataFrame({"status": ["open"]})
        with pytest.raises(KeyError):
            replace_column_values(df, columns=["missing"], value_map={}, errors="raise")

    def test_missing_column_warn_is_noop(self):
        df = pd.DataFrame({"status": ["open"]})
        result = replace_column_values(df, columns=["missing"], value_map={}, errors="warn")
        assert "missing" not in result.columns

    def test_invalid_errors_value_raises(self):
        df = pd.DataFrame({"status": ["open"]})
        with pytest.raises(ValueError):
            replace_column_values(df, columns=["status"], value_map={}, errors="bogus")
