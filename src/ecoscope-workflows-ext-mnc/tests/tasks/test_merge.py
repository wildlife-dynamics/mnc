import pandas as pd
import pytest

from ecoscope_workflows_ext_mnc.tasks._merge import merge_dataframes

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
