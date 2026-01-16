import pandas as pd
from typing import List
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame

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