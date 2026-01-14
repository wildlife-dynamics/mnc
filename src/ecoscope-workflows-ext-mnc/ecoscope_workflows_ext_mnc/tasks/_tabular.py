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

@task
def replace_missing_with_label(
    df: AnyDataFrame, 
    columns: str | list[str], 
    label: str
) -> AnyDataFrame:
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
        df[col] = (
            df[col]
            .replace(["None", "none", "Nan", "nan", "NaN", ""], None)  
            .fillna(label)
        )
    
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

@task 
def categorize_bins(df:AnyDataFrame, col:str)->AnyDataFrame:
    def extract_left(x):
        return int(re.search(r'^\d+', str(x)).group())
    
    sorted_bins = sorted(df[col].dropna().unique(), key=extract_left)
    df[col] = pd.Categorical(df[col], categories=sorted_bins, ordered=True)
    return df

@task 
def drop_null_values(df: AnyDataFrame, col: str)->AnyDataFrame:
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in dataframe.")
    return df.dropna(subset=[col])