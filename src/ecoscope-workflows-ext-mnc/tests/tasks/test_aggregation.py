import pandas as pd
import pytest

from ecoscope_workflows_ext_mnc.tasks.aggregation._aggregation import apply_arithmetic_operation_over_rows


class TestApplyArithmeticOperationOverRows:
    """Test cases for apply_arithmetic_operation_over_rows."""

    @pytest.fixture
    def df(self):
        return pd.DataFrame({"a": [10, 20, 30], "b": [1, 2, 3], "c": [2, 2, 2]})

    def test_add(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "b"], output_column="out", operation="add")
        assert result["out"].tolist() == [11, 22, 33]

    def test_subtract(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "b"], output_column="out", operation="subtract")
        assert result["out"].tolist() == [9, 18, 27]

    def test_multiply(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "b"], output_column="out", operation="multiply")
        assert result["out"].tolist() == [10, 40, 90]

    def test_divide(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "b"], output_column="out", operation="divide")
        assert result["out"].tolist() == pytest.approx([10.0, 10.0, 10.0])

    def test_floor_divide(self, df):
        result = apply_arithmetic_operation_over_rows(
            df, columns=["a", "b"], output_column="out", operation="floor_divide"
        )
        assert result["out"].tolist() == [10, 10, 10]

    def test_modulo(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "c"], output_column="out", operation="modulo")
        assert result["out"].tolist() == [0, 0, 0]

    def test_power(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["b", "c"], output_column="out", operation="power")
        assert result["out"].tolist() == [1, 4, 9]

    def test_min(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "c"], output_column="out", operation="min")
        assert result["out"].tolist() == [2, 2, 2]

    def test_max(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "c"], output_column="out", operation="max")
        assert result["out"].tolist() == [10, 20, 30]

    def test_combines_three_columns_in_order(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "b", "c"], output_column="out", operation="add")
        assert result["out"].tolist() == [13, 24, 35]

    def test_unknown_operation_raises_keyerror(self, df):
        with pytest.raises(KeyError):
            apply_arithmetic_operation_over_rows(df, columns=["a", "b"], output_column="out", operation="bogus")

    def test_missing_column_raises_keyerror(self, df):
        with pytest.raises(KeyError):
            apply_arithmetic_operation_over_rows(df, columns=["a", "missing"], output_column="out", operation="add")

    def test_mutates_input_in_place(self, df):
        result = apply_arithmetic_operation_over_rows(df, columns=["a", "b"], output_column="out", operation="add")
        assert result is df
        assert "out" in df.columns
