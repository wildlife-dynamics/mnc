import pandas as pd
from pydantic import Field
from wt_registry import register
from ecoscope.platform.annotations import AnyDataFrame
from typing import Union, List, Literal, Annotated, cast, Dict, Any


@register()
def remove_brackets_from_column(df: AnyDataFrame, columns: List) -> AnyDataFrame:
    if isinstance(columns, str):
        columns = [columns]

    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (None if isinstance(x, list) else x)
            )
    return df


@register()
def convert_columns_to_int(
    df: AnyDataFrame,
    columns: Union[str, List[str]],
    errors: Literal["coerce", "raise", "ignore"] = "coerce",
    fill_value: int = 0,
) -> AnyDataFrame:
    """Convert the given column(s) to integer dtype.

    errors:
        "coerce" - non-numeric values and NaN become `fill_value`.
        "raise"  - raise if a column cannot be converted.
        "ignore" - leave a column unchanged if conversion fails.
    """
    if errors not in {"coerce", "raise", "ignore"}:
        raise ValueError(f"errors must be 'coerce', 'raise', or 'ignore', got {errors!r}")

    if isinstance(columns, str):
        columns = [columns]

    df = df.copy()

    for column in columns:
        if column not in df.columns:
            msg = f"Column {column!r} not found in DataFrame."
            if errors == "raise":
                raise KeyError(msg)
            print("%s Skipping.", msg)
            continue

        try:
            if errors == "coerce":
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(fill_value).astype(int)
            else:  # "raise" or "ignore"
                df[column] = pd.to_numeric(df[column], errors="raise").astype(int)
        except (ValueError, TypeError) as e:
            if errors == "raise":
                raise
            print("Could not convert column %r to int: %s. Leaving unchanged.", column, e)

    return df


@register()
def fill_missing_values(
    df: AnyDataFrame,
    value: Annotated[
        str | int | float,
        Field(description="Constant value used to replace missing (NA/NaN) entries."),
    ],
    subset: Annotated[
        str | List[str] | None,
        Field(description="Column or columns to fill. If omitted, all columns are filled."),
    ] = None,
) -> AnyDataFrame:
    """Replace missing (NA/NaN) values with a constant.

    Returns a new DataFrame; the input is left unchanged.

    Args:
        df: The DataFrame to operate on.
        value: Constant used to fill missing entries.
        subset: Optional column or list of columns to restrict the fill to.
            When None, every column is filled.

    Returns:
        A copy of `df` with missing values replaced.
    """
    if subset is None:
        return cast(AnyDataFrame, df.fillna(value))

    columns = [subset] if isinstance(subset, str) else subset
    filled = df.copy()
    filled[columns] = filled[columns].fillna(value)
    return cast(AnyDataFrame, filled)


@register()
def replace_column_values(
    df: AnyDataFrame,
    columns: List[str],
    value_map: Dict[Any, Any],
    inplace: bool = False,
    errors: str = "warn",
) -> AnyDataFrame:
    """Replace values in one or more columns using a mapping.

    Unlike ``.map``, values not present in ``value_map`` are left unchanged.

    Args:
        columns: Columns to apply the replacement to.
        value_map: {old_value: new_value}. Keys/values may be any type.
        inplace: If False (default), operate on a copy and leave the input untouched.
        errors: What to do when a column is missing —
            "warn" (default), "raise", or "ignore".
    """
    if isinstance(columns, str):
        columns = [columns]

    if errors not in ("warn", "raise", "ignore"):
        raise ValueError(f"errors must be 'warn', 'raise', or 'ignore'; got {errors!r}")

    if not inplace:
        df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = df[column].replace(value_map)
        elif errors == "raise":
            raise KeyError(f"Column '{column}' not found in DataFrame.")
        elif errors == "warn":
            print(f"Column '{column}' not found in DataFrame. Skipping.")

    return df


@register()
def order_categorical_by_number(
    df: AnyDataFrame,
    columns: Union[str, List[str]],
    errors: Literal["raise", "ignore"] = "raise",
) -> AnyDataFrame:
    """Reorder existing bins column(s) by the first number in each label."""
    import re

    if isinstance(columns, str):
        columns = [columns]

    df = df.copy()

    for column in columns:
        if column not in df.columns:
            msg = f"Column {column!r} not found in DataFrame."
            if errors == "raise":
                raise KeyError(msg)
            print(f"Skipping, {msg}")
            continue

        col = df[column]
        if not isinstance(col.dtype, pd.CategoricalDtype):
            col = col.astype("category")

        ordered_cats = sorted(col.cat.categories, key=lambda x: float(re.findall(r"-?\d+\.?\d*", x)[0]))
        df[column] = col.cat.reorder_categories(ordered_cats, ordered=True)

    return df
