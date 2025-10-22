import geopandas as gpd
from typing import Union,cast
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame

@task
def filter_by_value(
    df: AnyDataFrame,
    column_name: str,
    value: Union[int, str,float] 
) -> AnyDataFrame:
    """
    Return a DataFrame containing only rows where the given column matches the specified value.
    """
    df_filtered = df[df[column_name] == value].copy()
    return cast(AnyDataFrame, df_filtered)
