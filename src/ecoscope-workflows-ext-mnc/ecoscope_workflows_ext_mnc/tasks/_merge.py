import pandas as pd
from typing import List
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame
from ecoscope_workflows_core.skip import SKIP_SENTINEL


@task
def merge_dataframes(
    left_df: AnyDataFrame,
    right_df: AnyDataFrame,
    on: str,
    how: str = "left",
    preserve_left_index: bool = False,
) -> AnyDataFrame:
    if preserve_left_index:
        # Add the index as a column temporarily
        index_name = '__original_index__'
        left_with_idx = left_df.copy()
        left_with_idx[index_name] = left_df.index
        
        # Perform the merge
        merged_df = pd.merge(left_with_idx, right_df, how=how, on=on)
        
        # Restore the index from the column
        merged_df = merged_df.set_index(index_name)
        merged_df.index.name = left_df.index.name  # Preserve original index name
        
        return merged_df
    else:
        merged_df = pd.merge(left_df, right_df, how=how, on=on)
        return merged_df
    
@task
def merge_multiple_df(
    list_df: List[AnyDataFrame], 
    ignore_index: bool = True, 
    sort: bool = False
) -> AnyDataFrame:
    """
    Merge multiple dataframes into a single dataframe.
    
    This function filters out any SkipSentinel values before merging,
    allowing workflows to continue even when some upstream tasks are skipped.

    Args:
        list_df: List of dataframes to concatenate. SkipSentinel values are ignored.
        ignore_index: If True, do not use the index values along the concatenation axis
        sort: Sort non-concatenation axis if it is not already aligned

    Returns:
        A single merged dataframe

    Raises:
        ValueError: If list_df is empty or contains only SkipSentinel values
        
    Examples:
        >>> df1 = pd.DataFrame({'a': [1, 2]})
        >>> df2 = pd.DataFrame({'a': [3, 4]})
        >>> result = merge_multiple_df([df1, df2])
        
        >>> # With SkipSentinel values
        >>> result = merge_multiple_df([df1, SKIP_SENTINEL, df2])  # Ignores SKIP_SENTINEL
    """
    if not list_df:
        raise ValueError("list_df cannot be empty")
    
    # Filter out SkipSentinel values
    valid_dfs = [df for df in list_df if df is not SKIP_SENTINEL]
    
    # Check if we have any valid dataframes after filtering
    if not valid_dfs:
        raise ValueError(
            "No valid dataframes to merge. All items in list_df are SkipSentinel values."
        )
    
    # Merge the valid dataframes
    merged_df = pd.concat(valid_dfs, ignore_index=ignore_index, sort=sort)
    
    return merged_df