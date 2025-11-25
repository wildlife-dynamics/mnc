# FIXME: This module provides aliased versions of tasks from 'ecoscope_workflows_ext_custom'.
# This is a temporary workaround to resolve naming collisions or validation issues
# that occur when tasks from 'ecoscope_workflows_ext_ecoscope' and 'ecoscope_workflows_ext_custom'
# have similar names or cause conflicts in direct-chain workflows.
# By aliasing, we ensure unique task identification and bypass potential validation errors.
# This workaround should be removed once the underlying library issues are resolved.
from typing import Annotated
from pydantic.json_schema import SkipJsonSchema
from pydantic import Field

from ecoscope_workflows_core.annotations import AnyGeoDataFrame, AdvancedField
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_ext_custom.tasks.results._map import (
    create_polygon_layer,
    PolygonLayerStyle,
    LegendDefinition,
    LayerDefinition,
    set_base_maps,
    TileLayer,
    _preset_or_custom_json_schema_extra,
)

@task
def create_polygon_layer_aliased(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="The geodataframe to visualize.", exclude=True),
    ],
    layer_style: Annotated[
        PolygonLayerStyle | SkipJsonSchema[None],
        AdvancedField(default=PolygonLayerStyle(), description="Style arguments for the layer."),
    ] = None,
    legend: Annotated[
        LegendDefinition | SkipJsonSchema[None],
        AdvancedField(
            default=None,
            description="If present, includes this layer in the map legend",
        ),
    ] = None,
) -> LayerDefinition:
    """
    A uniquely named alias for the standard `create_polygon_layer` task.
    This is a workaround for a suspected task name conflict or validation issue.
    """
    return create_polygon_layer.func(
        geodataframe=geodataframe,
        layer_style=layer_style,
        legend=legend,
    )

@task
def set_base_maps_aliased(
    base_maps: Annotated[
        list[TileLayer] | SkipJsonSchema[None],
        Field(
            json_schema_extra=_preset_or_custom_json_schema_extra,
            title=" ",
            description=(
                "Select tile layers to use as base layers in map outputs. "
                "The first layer in the list will be the bottommost layer displayed."
            ),
        ),
    ] = None,
) -> Annotated[list[TileLayer], Field()]:
    """
    A uniquely named alias for the standard `set_base_maps` task.
    This is a workaround for a suspected task name conflict or validation issue.
    """
    return set_base_maps.func(base_maps=base_maps)