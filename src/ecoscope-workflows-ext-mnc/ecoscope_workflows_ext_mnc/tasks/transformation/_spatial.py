from wt_registry import register
from pydantic import Field
from typing import Annotated, Literal, List, Dict
from ecoscope.platform.annotations import AnyDataFrame, AnyGeoDataFrame


@register()
def build_legend_values_from_column(
    df: Annotated[
        AnyDataFrame,
        Field(description="Dataframe holding the label and per-row color columns.", exclude=True),
    ],
    label_column: Annotated[str, Field(description="Column with the category label per row.")],
    color_column: Annotated[str, Field(description="Column with the per-row color (hex string or RGBA tuple/list).")],
    sort: Annotated[
        Literal["ascending", "descending"] | None,
        Field(description="Sort order for the legend entries by label."),
    ] = "ascending",
) -> List[Dict[str, str]]:
    """Build a static {label, color} legend list from unique values in a dataframe column.

    `draw_map`'s dataframe-lookup legend (`label_column`/`color_column`) only works when
    it's evaluated against the same geodataframe it was defined on. `combine_deckgl_map_layers`
    re-attaches a static layer's legend onto a copy of the grouped layer for rendering, so a
    lookup-style legend on a static layer ends up looking up its columns on the wrong
    dataframe. Computing the entries here, eagerly, avoids that mismatch.
    """
    lookup = df.drop_duplicates(subset=label_column)[[label_column, color_column]]
    if sort:
        lookup = lookup.sort_values(label_column, ascending=(sort == "ascending"))

    def _to_css(color) -> str:
        if isinstance(color, str):
            return color
        r, g, b, a = color
        return f"rgba({int(r)}, {int(g)}, {int(b)}, {a / 255})"

    return [{"label": str(row[label_column]), "color": _to_css(row[color_column])} for _, row in lookup.iterrows()]


@register()
def fix_invalid_geometries(
    gdf: Annotated[
        AnyGeoDataFrame,
        Field(description="GeoDataFrame whose geometries should be repaired."),
    ],
) -> AnyGeoDataFrame:
    """Repair invalid geometries (e.g. self-intersecting polygons) in place.

    Digitized boundary/shapefile data commonly contains topologically invalid
    geometries that raise a GEOSException ("TopologyException: side location
    conflict...") when passed to operations like `union_all()`. This runs
    shapely's `make_valid()` over any invalid geometry and leaves valid ones
    untouched.
    """
    gdf = gdf.copy()
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        gdf.loc[invalid, gdf.geometry.name] = gdf.loc[invalid, gdf.geometry.name].make_valid()
    return gdf
