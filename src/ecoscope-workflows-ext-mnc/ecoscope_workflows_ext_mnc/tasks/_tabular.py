import re
import ast
import warnings
import numpy as np
import pandas as pd
import matplotlib as mpl
from pydantic import Field
from ecoscope.base.utils import hex_to_rgba
from typing import Union, List,Dict,Annotated
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame

@task
def add_totals_row(
    df: AnyDataFrame,
    label_col: Union[str, List[str], None] = None,
    label: str = "Total"
) -> AnyDataFrame:
    totals = df.select_dtypes(include="number").sum(numeric_only=True)
    total_row = pd.Series({col: None for col in df.columns})
    total_row[totals.index] = totals.values

    if isinstance(label_col, str):
        if label_col in df.columns:
            total_row[label_col] = label
    elif isinstance(label_col, list):
        for col in label_col:
            if col in df.columns:
                total_row[col] = label
    return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

@task
def map_column_values(
    df: AnyDataFrame,
    columns: List[str],
    value_map: Dict[str, str],
    inplace: bool = False
) -> AnyDataFrame:
    if not inplace:
        df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = df[column].replace(value_map)
        else:
            warnings.warn(f"Column '{column}' not found in DataFrame. Skipping.")
    
    return df

# @task
# def replace_missing_with_label(
#     df: AnyDataFrame, 
#     columns: str | list[str], 
#     label: str
# ) -> AnyDataFrame:
#     # Convert single column to list for uniform processing
#     columns = [columns] if isinstance(columns, str) else columns
    
#     # Validate all columns exist
#     missing_cols = [col for col in columns if col not in df.columns]
#     if missing_cols:
#         raise ValueError(
#             f"Column(s) {missing_cols} not found. "
#             f"Available columns: {list(df.columns)}"
#         )
    
#     df = df.copy()
    
#     # Apply replacement to each column
#     for col in columns:
#         df[col] = (
#             df[col]
#             .replace(["None", "none", "Nan", "nan", "NaN", ""], None)  
#             .fillna(label)
#         )
    
#     return df

@task
def replace_missing_with_label(
    df: AnyDataFrame, 
    columns: str | list[str], 
    label: str
) -> AnyDataFrame:
    """
    Replace all forms of missing/null values with a specified label.
    
    Catches:
    - pd.NA, np.nan, None
    - String representations: "None", "nan", "NaN", "Nan", etc.
    - Empty strings and whitespace-only strings
    - The string "NaN" (case-insensitive)
    """
    # Convert single column to list for uniform processing
    columns = [columns] if isinstance(columns, str) else columns
    
    # Validate all columns exist
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Column(s) {missing_cols} not found. "
            f"Available columns: {list(df.columns)}"
        )
    
    df = df.copy()
    
    # Apply replacement to each column
    for col in columns:
        # First, replace string representations of null with actual None
        df[col] = df[col].replace(
            ["None", "none", "NONE", 
             "Nan", "nan", "NaN", "NAN",
             "null", "NULL", "Null",
             "", " ", "  "],  # Empty and whitespace strings
            None
        )
        
        # Then handle actual pandas NA/NaN/None values
        # Use pd.isna() which catches pd.NA, np.nan, None, pd.NaT
        df[col] = df[col].where(~pd.isna(df[col]), label)
        
        # Additional safety: catch any remaining string "nan" that might have been created
        df[col] = df[col].replace("nan", label)
    
    return df

@task
def convert_to_int(
    df: AnyDataFrame,
    columns: Union[str, List[str]],
    errors: str = 'coerce',
    fill_value: int = 0,
    inplace: bool = False
) -> AnyDataFrame:
    if not inplace:
        df = df.copy()
    if isinstance(columns, str):
        columns = [columns]
    
    for column in columns:
        if column not in df.columns:
            print(f"Warning: Column '{column}' not found in DataFrame. Skipping.")
            continue
        
        try:
            if errors == 'coerce':
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(fill_value).astype(int)
            else:
                df[column] = df[column].astype(int)
        except Exception as e:
            print(f"Error converting column '{column}' to int: {e}")
            if errors == 'raise':
                raise
    
    return df


@task
def to_sentence_case(
    df: AnyDataFrame, 
    columns: str | list[str]
) -> AnyDataFrame:
    if df.empty:
        raise ValueError("DataFrame is empty.")
    
    # Convert single column to list for uniform processing
    columns = [columns] if isinstance(columns, str) else columns
    
    # Validate all columns exist
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Column(s) {missing_cols} not found in DataFrame. "
            f"Available columns: {list(df.columns)}"
        )
    
    # Apply sentence case to each column
    for col in columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.capitalize()
        )
    
    return df

def create_bins(series: pd.Series, bins: int = 5, min_start: int = 0) -> pd.Series:
    """
    Create whole-number bins for a numeric pandas Series.
    - Forces values <= 0 to NaN (excluded from bins).
    - Builds integer edges so labels are unique.
    - If rounding collapses edges, reduces number of bins automatically.
    - Returns string labels like "1–5" and None for missing values.
    """
    s = pd.to_numeric(series, errors="coerce")
    s_pos = s.where(s > 0, np.nan)
    if s_pos.dropna().empty:
        return pd.Series([None] * len(s_pos), index=s_pos.index, dtype=object)

    raw_min = int(np.floor(s_pos.min()))
    raw_max = int(np.ceil(s_pos.max()))
    start = max(min_start, raw_min)
    end = max(start + 1, raw_max)
    desired_edges = np.linspace(start, end, bins + 1)
    int_edges = np.unique(np.round(desired_edges).astype(int))
    if int_edges.size < 2:
        int_edges = np.array([start, end], dtype=int)
    effective_bins = int_edges.size - 1
    if effective_bins <= 0:
        int_edges = np.array([start, start + 1], dtype=int)
        effective_bins = 1

    cat = pd.cut(s_pos, bins=int_edges, include_lowest=True)

    labels = []
    for left, right in zip(int_edges[:-1], int_edges[1:]):
        r_display = max(right, left + 1)
        labels.append(f"{int(left)}–{int(r_display)}")

    if len(labels) == len(cat.cat.categories):
        cat = cat.cat.rename_categories(labels)
    else:
        cat = cat.astype(object).where(~cat.isna(), other=None)
        return cat
    out = cat.astype(object).where(~cat.isna(), other=None)
    out.index = s_pos.index
    return out

@task
def bin_columns(
    df: AnyDataFrame, 
    columns: Union[str, List[str]], 
    bins: int = 5,
    suffix: str = " bins",
    inplace: bool = False
) -> AnyDataFrame:
    """
    Create binned versions of numeric columns.
    
    Args:
        df: Input DataFrame
        columns: Column name (str) or list of column names to bin
        bins: Number of bins to create (default 5)
        suffix: Suffix to add to new column names (default " bins")
        inplace: Whether to modify DataFrame in place
    
    Returns:
        DataFrame with new binned columns
    """
    if not inplace:
        df = df.copy()
    
    # Convert single column to list
    if isinstance(columns, str):
        columns = [columns]
    
    for column in columns:
        if column not in df.columns:
            print(f"Warning: Column '{column}' not found in DataFrame. Skipping.")
            continue
        
        try:
            new_col = f"{column}{suffix}"
            df[new_col] = create_bins(df[column], bins=bins)
        except Exception as e:
            print(f"Error binning column '{column}': {e}")
            continue
    
    return df


@task
def order_bins(df:AnyDataFrame, col:str)->AnyDataFrame:
    def extract_left(x):
        return int(re.search(r'^\d+', str(x)).group())
    sorted_bins = sorted(df[col].dropna().unique(), key=extract_left)
    df[col] = pd.Categorical(df[col], categories=sorted_bins, ordered=True)
    return df

# @task 
# def categorize_bins(df:AnyDataFrame, col:str)->AnyDataFrame:
#     def extract_left(x):
#         return int(re.search(r'^\d+', str(x)).group())
    
#     sorted_bins = sorted(df[col].dropna().unique(), key=extract_left)
#     df[col] = pd.Categorical(df[col], categories=sorted_bins, ordered=True)

#     #convert the categorical col to str 
#     df[col] = df[col].astype(str)
    
#     return df

# @task 
# def categorize_bins(df: AnyDataFrame, col: str) -> AnyDataFrame:
#     """
#     Categorize bins and return clean string column with preserved order.
#     Uses a helper column to maintain sort order.
#     """
#     def extract_left(x):
#         match = re.search(r'^\d+', str(x))
#         return int(match.group()) if match else -1
    
#     df_copy = df.copy()
    
#     # Create a sort order column based on the left bound
#     df_copy[f'{col}_sort'] = df_copy[col].apply(extract_left)
    
#     # Convert to string (for colormap compatibility)
#     df_copy[col] = df_copy[col].astype(str)
    
#     return df_copy

@task 
def categorize_bins(df: AnyDataFrame, col: str) -> AnyDataFrame:
    """
    Categorize bins and return clean string column with preserved order.
    Uses a helper column to maintain sort order.
    Filters out rows where bin values are non-positive or non-numeric.
    
    Handles various bin formats:
    - Interval notation: (0, 10], [10, 20), etc.
    - Simple ranges: 0-10, 10-20
    - Plain numbers: 1, 10, 20
    """
    def extract_left(x):
        """Extract the leftmost numeric value from various bin formats."""
        if pd.isna(x) or x is None:
            return None
        
        s = str(x)
        match = re.search(r'-?\d+\.?\d*', s)
        
        if match:
            try:
                value = float(match.group())
                # Return None for non-positive values
                return value if value > 0 else None
            except ValueError:
                return None
        
        return None
    
    df_copy = df.copy()
    
    # Create a sort order column based on the left bound
    df_copy[f'{col}_sort'] = df_copy[col].apply(extract_left)
    df_copy = df_copy[df_copy[f'{col}_sort'].notna()].copy()
    #df_copy[col] = df_copy[col].astype(str)
    sorted_bins = sorted(df[col].dropna().unique(), key=extract_left)
    df[col] = pd.Categorical(df[col], categories=sorted_bins, ordered=True)
    
    return df_copy


@task 
def drop_null_values(df: AnyDataFrame, col: str)->AnyDataFrame:
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in dataframe.")
    return df.dropna(subset=[col])

@task 
def remove_substring(df:AnyDataFrame, column:str, value:str)->AnyDataFrame:
    if df.empty:
        raise ValueError("DataFrame is empty.")
    if column not in df.columns:
        raise ValueError(f"{column} not found in DataFrame.")

    # remove substring (case-insensitive)
    df[column] = df[column].str.replace(value, " ", case=False, regex=False)

    # remove leftover spaces
    df[column] = df[column].str.strip()

    return df

@task
def remove_brackets_from_column(df:AnyDataFrame, columns:List)->AnyDataFrame:
    if isinstance(columns, str):
        columns = [columns] 

    for col in columns:
        df[col] = df[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    return df

@task
def pivot_df(
    df:AnyDataFrame, 
    index_col:str, 
    columns_col=str, 
    values_col=str,
    reset_idx=True
    )->AnyDataFrame:
    result = df.pivot(
        index=index_col, 
        columns=columns_col, 
        values=values_col
        )
    
    if reset_idx:
        result = result.reset_index()
    
    return result


@task
def clean_dataframe_index(
    df: AnyDataFrame,
    reset_index: bool = True,
    drop_index: bool = True,
    rename_unnamed: bool = True,
    unnamed_col_name: str = "idx"
) -> AnyDataFrame:
    """
    Cleans dataframe by resetting index and/or renaming unnamed columns.
    """
    df_clean = df.copy()
    
    # Reset index FIRST if requested
    if reset_index:
        df_clean = df_clean.reset_index(drop=drop_index)
    
    # THEN rename unnamed columns
    if rename_unnamed:
        new_columns = []
        unnamed_count = 0
        
        for col in df_clean.columns:
            # Handle None, empty string, or common unnamed patterns
            if (col is None or 
                str(col).strip() == '' or 
                str(col).startswith('Unnamed:') or 
                str(col) in ['index', 'level_0']):
                
                new_name = f"{unnamed_col_name}_{unnamed_count}" if unnamed_count > 0 else unnamed_col_name
                new_columns.append(new_name)
                unnamed_count += 1
            else:
                new_columns.append(col)
        
        df_clean.columns = new_columns
    
    return df_clean


@task 
def map_name_values(df: AnyDataFrame, column:str)-> AnyDataFrame:
    """
    Convert strings like 'ab_cd' to 'Ab Cd' in a given dataframe column.
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    
    df[column] = (
        df[column]
        .astype(str)              # ensure string
        .str.replace("_", " ")    # swap underscores
        .str.lower()              # make all lower
        .str.title()              # capitalize each word
    )
    return df


@task
def filter_non_empty_values(df: AnyDataFrame, column: str) -> AnyDataFrame:
    """
    Filter dataframe to keep only rows where the specified column has non-empty values.
    
    Works with lists, strings, dicts, sets, or any type where len() > 0 indicates non-empty.
    
    Args:
        df: Input DataFrame
        column: Name of column to check for non-empty values
        
    Returns:
        Filtered DataFrame with only non-empty values in the specified column
        
    Examples:
        >>> df = pd.DataFrame({
        ...     'id': [1, 2, 3, 4],
        ...     'patrols': [['a', 'b'], [], ['c'], None]
        ... })
        >>> filtered = filter_non_empty_values(df, 'patrols')
        >>> len(filtered)
        2
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    filtered_df = df[df[column].map(lambda x: len(x) if x is not None and hasattr(x, '__len__') else 0) > 0]
    
    return filtered_df.reset_index(drop=True)


def safe_literal_eval(val):
    try:
        return ast.literal_eval(val) if isinstance(val, str) else val
    except (ValueError, SyntaxError):
        return [val]
    
@task
def explode_multiple_columns(
    df: AnyDataFrame, 
    columns: Union[str, List[str]], 
    reset_index: bool = True
) -> AnyDataFrame:
    """
    Explode one or multiple columns in a DataFrame sequentially.
    
    This function takes list-like values in specified columns and expands them
    into separate rows. When multiple columns are provided, they are exploded
    sequentially (first column, then second column, etc.).
    
    Args:
        df: Input DataFrame
        columns: Single column name or list of column names to explode
        reset_index: Whether to reset the index after exploding (default: True)
        
    Returns:
        DataFrame with exploded columns
        
    Examples:
        >>> df = pd.DataFrame({
        ...     'id': [1, 2],
        ...     'patrols': [['a', 'b'], ['c']],
        ...     'participants': [[1, 2], [3]]
        ... })
        >>> result = explode_multiple_columns(df, ['patrols', 'participants'])
    """
    
    if isinstance(columns, str):
        columns = [columns]
    
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
    
    result_df = df.copy()
    for col in columns:
        result_df[col] = result_df[col].apply(safe_literal_eval)
        result_df = result_df.explode(col)
    
    if reset_index:
        result_df = result_df.reset_index(drop=False)
    
    return result_df

@task
def round_values(df: AnyDataFrame, column: str, decimals) -> AnyDataFrame:
    if df.empty:
        raise ValueError("Input DataFrame is empty.")

    if column not in df.columns:
        raise ValueError(f"Column '{column}' does not exist in the DataFrame.")

    try:
        decimals = int(decimals)
    except (TypeError, ValueError):
        raise TypeError("Parameter 'decimals' must be a numeric value.")

    if not pd.api.types.is_numeric_dtype(df[column]):
        raise TypeError(f"Column '{column}' must be numeric.")

    df = df.copy()
    df[column] = df[column].round(decimals)

    return df
