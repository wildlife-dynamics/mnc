from typing import cast
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_ext_ecoscope.connections import EarthRangerClient
from ecoscope_workflows_ext_ecoscope.tasks.io._earthranger import (
    PatrolsDF,
    EmptyDataFrame,
    PatrolObservationsGDF,
    IncludePatrolDetailsAnnotation,
    RaiseOnEmptyAnnotation,
    SubPageSizeAnnotation,
)


@task
def custom_get_patrol_observations_from_patrols_df(
    client: EarthRangerClient,
    patrols_df: PatrolsDF,
    include_patrol_details: IncludePatrolDetailsAnnotation = True,
    raise_on_empty: RaiseOnEmptyAnnotation = True,
    sub_page_size: SubPageSizeAnnotation = 100,
) -> PatrolObservationsGDF | EmptyDataFrame:
    """Get observations for a patrol type from EarthRanger."""
    from ecoscope.relocations import Relocations  # type: ignore[import-untyped]

    patrol_obs_relocs = client.get_patrol_observations(
        patrols_df=patrols_df,
        include_patrol_details=include_patrol_details,
        sub_page_size=sub_page_size,
    )
    if isinstance(patrol_obs_relocs, Relocations):
        patrol_obs_relocs = patrol_obs_relocs.gdf

    if raise_on_empty and patrol_obs_relocs.empty:
        raise ValueError("No data returned from EarthRanger for get_patrol_observations_with_patrol_filter")

    return cast(PatrolObservationsGDF, patrol_obs_relocs)
