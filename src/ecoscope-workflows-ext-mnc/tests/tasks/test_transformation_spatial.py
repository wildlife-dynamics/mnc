import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from ecoscope_workflows_ext_mnc.tasks.transformation._spatial import (
    build_legend_values_from_column,
    fix_invalid_geometries,
)


class TestBuildLegendValuesFromColumn:
    """Test cases for build_legend_values_from_column."""

    def test_dedupes_and_defaults_to_ascending(self):
        df = pd.DataFrame(
            {
                "label": ["Zebra", "Lion", "Zebra"],
                "color": ["#ffffff", "#000000", "#ffffff"],
            }
        )
        result = build_legend_values_from_column(df, label_column="label", color_column="color")
        assert result == [
            {"label": "Lion", "color": "#000000"},
            {"label": "Zebra", "color": "#ffffff"},
        ]

    def test_descending_sort(self):
        df = pd.DataFrame({"label": ["Lion", "Zebra"], "color": ["#000000", "#ffffff"]})
        result = build_legend_values_from_column(df, label_column="label", color_column="color", sort="descending")
        assert [entry["label"] for entry in result] == ["Zebra", "Lion"]

    def test_sort_none_preserves_first_occurrence_order(self):
        df = pd.DataFrame({"label": ["Zebra", "Lion"], "color": ["#ffffff", "#000000"]})
        result = build_legend_values_from_column(df, label_column="label", color_column="color", sort=None)
        assert [entry["label"] for entry in result] == ["Zebra", "Lion"]

    def test_rgba_tuple_converted_to_css(self):
        df = pd.DataFrame({"label": ["A"], "color": [(255, 0, 0, 255)]})
        result = build_legend_values_from_column(df, label_column="label", color_column="color")
        assert result == [{"label": "A", "color": "rgba(255, 0, 0, 1.0)"}]

    def test_rgba_list_with_partial_alpha(self):
        df = pd.DataFrame({"label": ["A"], "color": [[0, 128, 64, 128]]})
        result = build_legend_values_from_column(df, label_column="label", color_column="color")
        assert result == [{"label": "A", "color": f"rgba(0, 128, 64, {128 / 255})"}]

    def test_single_row(self):
        df = pd.DataFrame({"label": ["Only"], "color": ["#abcdef"]})
        result = build_legend_values_from_column(df, label_column="label", color_column="color")
        assert result == [{"label": "Only", "color": "#abcdef"}]


class TestFixInvalidGeometries:
    """Test cases for fix_invalid_geometries."""

    def test_valid_geometries_left_unchanged(self):
        valid = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
        gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[valid])
        result = fix_invalid_geometries(gdf)
        assert result.geometry.is_valid.all()
        assert result.geometry.iloc[0].equals(valid)

    def test_invalid_geometry_is_repaired(self):
        # Self-intersecting "bowtie" polygon.
        bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
        assert not bowtie.is_valid
        gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[bowtie])
        result = fix_invalid_geometries(gdf)
        assert result.geometry.is_valid.all()

    def test_mixed_valid_and_invalid(self):
        valid = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
        bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
        gdf = gpd.GeoDataFrame({"id": [1, 2]}, geometry=[valid, bowtie])
        result = fix_invalid_geometries(gdf)
        assert result.geometry.is_valid.all()
        assert result.geometry.iloc[0].equals(valid)

    def test_does_not_mutate_input(self):
        bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
        gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[bowtie])
        fix_invalid_geometries(gdf)
        assert not gdf.geometry.iloc[0].is_valid
