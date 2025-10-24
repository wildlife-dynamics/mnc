import os
import uuid
import json
import pandas as pd
from pathlib import Path
from docx.shared import Cm
from datetime import datetime
from typing import Optional, Union
from dataclasses import dataclass, asdict
from docxtpl import DocxTemplate, InlineImage
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame
from ecoscope_workflows_core.tasks.filter._filter import TimeRange

def normalize_file_url(path: str) -> str:
    """Convert file:// URL to local path, handling malformed Windows URLs."""
    if not path.startswith("file://"):
        return path

    path = path[7:]
    
    if os.name == 'nt':
        # Remove leading slash before drive letter: /C:/path -> C:/path
        if path.startswith('/') and len(path) > 2 and path[2] in (':', '|'):
            path = path[1:]
        path = path.replace('/', '\\')
        path = path.replace('|', ':')
    else:
        if not path.startswith('/'):
            path = '/' + path
    
    return path

def _load_df(df: Union[str, Path, AnyDataFrame]) -> AnyDataFrame:
    """Load DataFrame from file path or return existing DataFrame."""
    # Check if it's already a DataFrame
    if isinstance(df, pd.DataFrame):
        return df
    
    # If it's None, return empty DataFrame
    if df is None:
        return pd.DataFrame()
    
    # Normalize the path and convert to Path object
    normalized_path = normalize_file_url(str(df))
    p = Path(normalized_path)
    
    if not p.exists():
        print(f"Warning: File not found: {p}")
        return pd.DataFrame()
    
    if p.suffix.lower() in {".csv"}:
        return pd.read_csv(p)
    elif p.suffix.lower() in {".parquet"}:
        return pd.read_parquet(p)
    else:
        return pd.read_csv(p)

def _safe_extract_value(df: pd.DataFrame, column: str, default= 0):
    """Safely extract a value from DataFrame column."""
    if df is None or df.empty:
        return default
    
    if column not in df.columns:
        print(f"Warning: Column '{column}' not found in DataFrame")
        return default
    
    try:
        value = df[column].iloc[0]
        return value if pd.notna(value) else default
    except (IndexError, KeyError):
        return default

@dataclass
class MncContext:
    """Data class to hold all context variables for MNC report generation."""
    time_period: Optional[str] = None
    time_range: Optional[str] = None
    temperature_chart: Optional[str] = None
    precipitation_chart: Optional[str] = None
    total_events_chart: Optional[str] = None
    total_events: Optional[str] = None
    
    # Foot patrols
    foot_patrols_map: Optional[str] = None
    no_of_foot_patrols: Optional[str] = None
    total_foot_patrol_hours: Optional[str] = None
    total_foot_patrol_distance: Optional[str] = None
    
    # Vehicle patrols
    vehicle_patrols_map: Optional[str] = None
    no_of_vehicle_patrol: Optional[str] = None
    total_vehicle_patrol_hours: Optional[str] = None
    total_vehicle_patrol_distance: Optional[str] = None
    average_vehicle_patrol_speed: Optional[str] = None
    
    # Motorbike patrols
    motorbike_patrols_map: Optional[str] = None
    no_of_motor_patrol: Optional[str] = None
    total_motor_patrol_hours: Optional[str] = None
    total_motor_patrol_distance: Optional[str] = None
    average_motor_patrol_speed: Optional[str] = None
    
    # Coverage and summaries
    patrols_coverage_map: Optional[str] = None
    patrol_purpose_summary_dict: Optional[dict] = None
    patrol_effort_summary_dict: Optional[dict] = None

    # Patrol Purpose info
    night_patrols_percent: Optional[str] = None
    routine_patrols_percent: Optional[str] = None
    joint_patrols_percent: Optional[str] = None

    er_user: Optional[str]= None
    generated_on: Optional[str]=None


@task
def create_mnc_context(
    generated_by: str,
    template_path: str,
    output_dir: str,
    total_events_df: Optional[str] = None,
    foot_patrols_summary_df: Optional[str] = None,
    vehicle_patrols_summary_df: Optional[str] = None,
    motor_patrols_summary_df: Optional[str] = None,
    patrol_purpose_summary_df: Optional[str] = None,
    patrol_effort_summary_df: Optional[str] = None,
    temperature_chart: Optional[str] = None,
    precipitation_chart: Optional[str] = None,
    total_events_chart: Optional[str] = None,
    foot_patrols_map: Optional[str] = None,
    vehicle_patrols_map: Optional[str] = None,
    motorbike_patrols_map: Optional[str] = None,
    patrols_coverage_map: Optional[str] = None,
    validate_images: bool = True,
    box_h_cm: float = 6.5,
    box_w_cm: float = 11.11,
    time_period: Optional[TimeRange] = None,
    filename: Optional[str] = None,
) -> str:
    """
    Create MNC context document from template and data.
    
    Returns:
        str: Path to generated document
    """
    print("=" * 80)
    print("STARTING MNC CONTEXT GENERATION")
    print("=" * 80)
    
    # Normalize paths
    template_path = normalize_file_url(template_path)
    output_dir = normalize_file_url(output_dir)

    print(f"\nTemplate Path: {template_path}")
    print(f"Output Directory: {output_dir}")
    
    # Validate paths
    if not template_path.strip():
        raise ValueError("template_path is empty after normalization")
    if not output_dir.strip():
        raise ValueError("output_directory is empty after normalization")
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Load all DataFrames
    total_events_loaded = _load_df(total_events_df)
    foot_patrols_summary_loaded = _load_df(foot_patrols_summary_df)
    vehicle_patrols_summary_loaded = _load_df(vehicle_patrols_summary_df)
    motor_patrols_summary_loaded = _load_df(motor_patrols_summary_df)
    patrol_purpose_summary_loaded = _load_df(patrol_purpose_summary_df)
    patrol_effort_summary_loaded = _load_df(patrol_effort_summary_df)
    
    # Generate output filename
    if not filename:
        filename = f"mnc_report_{uuid.uuid4().hex[:8]}.docx"
    output_path = Path(output_dir) / filename
    print(f"Output File: {output_path}")
    
    # Process time period
    time_period_str = None
    duration_period_str = None
    if time_period:
        fmt = getattr(time_period, "time_format", "%Y-%m-%d")
        time_period_str = f"{time_period.since.strftime(fmt)} to {time_period.until.strftime(fmt)}"
        # Duration period - dates only
        date_fmt = "%Y-%m-%d"
        duration_period_str = f"{time_period.since.strftime(date_fmt)} to {time_period.until.strftime(date_fmt)}"
        print(f"\nTime Range: {time_period_str}")
        print(f"Duration Period: {duration_period_str}")
    
    # Extract total events
    print("\n" + "=" * 80)
    print("PROCESSING DATA")
    print("=" * 80)
    

    if not total_events_loaded.empty:
        # Identify the row where the date column equals "Total"
        total_row = total_events_loaded[
            total_events_loaded.iloc[:, 0].astype(str).str.strip().str.lower() == "Total"
        ]

        if not total_row.empty:
            # Extract the value in the second column (the total count)
            total_events_recorded = str(total_row.iloc[0, 1])
        else:
            # Fallback: Sum all numeric columns if "Total" row is not found
            total_events_recorded = str(total_events_loaded.select_dtypes(include="number").sum().sum())
    else:
        total_events_recorded = "0"

    print(f"\nTotal Events: {total_events_recorded}")

    
    # Extract foot patrol data
    print("\nFOOT PATROLS:")
    foot_patrol_count = str(int(_safe_extract_value(foot_patrols_summary_loaded, "no_of_patrols", 0))) # convert to int then str
    foot_patrol_hours = str(round(float(_safe_extract_value(foot_patrols_summary_loaded, "duration_hrs", 0)), 2))
    foot_patrol_distance = str(round(float(_safe_extract_value(foot_patrols_summary_loaded, "distance_km", 0)), 2))
    print(f"  • Count: {foot_patrol_count}")
    print(f"  • Hours: {foot_patrol_hours}")
    print(f"  • Distance: {foot_patrol_distance} km")
    
    # Extract vehicle patrol data
    print("\nVEHICLE PATROLS:")
    vehicle_patrol_count = str(int(_safe_extract_value(vehicle_patrols_summary_loaded, "no_of_patrols", 0)))# convert to int then str
    vehicle_patrol_hours = str(round(float(_safe_extract_value(vehicle_patrols_summary_loaded, "duration_hrs", 0)), 2))
    vehicle_patrol_distance = str(round(float(_safe_extract_value(vehicle_patrols_summary_loaded, "distance_km", 0)), 2))
    average_vehicle_speed = str(round(float(_safe_extract_value(vehicle_patrols_summary_loaded, "average_speed", 0)), 2))
    
    print(f"  • Count: {vehicle_patrol_count}")
    print(f"  • Hours: {vehicle_patrol_hours}")
    print(f"  • Distance: {vehicle_patrol_distance} km")
    print(f"  • Avg Speed: {average_vehicle_speed} km/h")
    
    # Extract motorbike patrol data
    print("\nMOTORBIKE PATROLS:")
    motor_patrol_count = str(int(_safe_extract_value(motor_patrols_summary_loaded, "no_of_patrols", 0))) # convert to int then str
    motor_patrol_hours = str(round(float(_safe_extract_value(motor_patrols_summary_loaded, "duration_hrs", 0)), 2))
    motor_patrol_distance = str(round(float(_safe_extract_value(motor_patrols_summary_loaded, "distance_km", 0)), 2))
    average_motor_speed = str(round(float(_safe_extract_value(motor_patrols_summary_loaded, "average_speed", 0)), 2))
    
    print(f"  • Count: {motor_patrol_count}")
    print(f"  • Hours: {motor_patrol_hours}")
    print(f"  • Distance: {motor_patrol_distance} km")
    print(f"  • Avg Speed: {average_motor_speed} km/h")
    
    def _detect_label_and_count_columns(df: pd.DataFrame):
        if df is None or df.empty:
            return None, None
        cols = list(df.columns)
        label_col = None
        count_col = None

        # find a good label column (heuristic)
        for c in cols:
            low = c.lower()
            if any(k in low for k in ["purpose", "patrol", "event_details", "event", "date", "label"]):
                label_col = c
                break

        # find a good count column (numeric or looks like counts)
        for c in cols:
            if pd.api.types.is_numeric_dtype(df[c]):
                count_col = c
                break
        if count_col is None:
            # fallback: a column whose name contains 'number' or 'count' or 'patrols'
            for c in cols:
                low = c.lower()
                if any(k in low for k in ["number", "count", "patrol", "patrols"]):
                    count_col = c
                    break

        # final fallbacks
        if label_col is None and len(cols) >= 1:
            label_col = cols[0]
        if count_col is None and len(cols) >= 2:
            count_col = cols[1]

        return label_col, count_col


    label_col, count_col = _detect_label_and_count_columns(patrol_purpose_summary_loaded)

    night_pct_str = "0%"
    routine_pct_str = "0%"
    joint_pct_str = "0%"

    if label_col and count_col and not patrol_purpose_summary_loaded.empty:
        df = patrol_purpose_summary_loaded.copy()
        # Normalize label column to string and the count column to numeric
        df[label_col] = df[label_col].astype(str).str.strip()
        df[count_col] = pd.to_numeric(df[count_col], errors="coerce")  # NaN for bad values

        # try to find a Total row (case-insensitive)
        total_row = df[df[label_col].str.strip().str.lower() == "total"]
        if not total_row.empty and pd.notna(total_row[count_col].iloc[0]):
            total_value = int(total_row[count_col].iloc[0])
        else:
            # fallback: sum numeric column (skip NaNs)
            total_value = int(df[count_col].fillna(0).sum())

        def _pct_for(pat_label: str) -> str:
            row = df[df[label_col].str.strip().str.lower() == pat_label.lower()]
            if row.empty or pd.isna(row[count_col].iloc[0]):
                return "0%"
            val = int(row[count_col].iloc[0])
            if total_value == 0:
                return f"{val} (0%)"
            pct = round((val / total_value) * 100, 2)
            return f"{val} ({pct}%)"

        night_pct_str = _pct_for("night")
        routine_pct_str = _pct_for("routine")
        joint_pct_str = _pct_for("joint")

        print("\nPATROL PURPOSE PERCENTAGES:")
        print(f"  • Night:   {night_pct_str} of total {total_value}")
        print(f"  • Routine: {routine_pct_str} of total {total_value}")
        print(f"  • Joint:   {joint_pct_str} of total {total_value}")
    else:
        print("\nPATROL PURPOSE PERCENTAGES: No valid data to compute percentages")


    # Convert summary DataFrames to dictionaries
    print("\nPATROL SUMMARIES:")
    # save these dicts on output dir as well
    patrol_purpose_summary_dict = (
        patrol_purpose_summary_loaded.to_dict('records') 
        if not patrol_purpose_summary_loaded.empty 
        else []
    )

    
    patrol_effort_summary_dict = (
        patrol_effort_summary_loaded.to_dict('records') 
        if not patrol_effort_summary_loaded.empty 
        else []
    )
    # Save JSON versions so they are available on output_dir
    try:
        with open(Path(output_dir) / 'patrol_purpose_summary.json', 'w', encoding='utf-8') as fh:
            json.dump(patrol_purpose_summary_dict, fh, ensure_ascii=False, indent=2)
        with open(Path(output_dir) / 'patrol_effort_summary.json', 'w', encoding='utf-8') as fh:
            json.dump(patrol_effort_summary_dict, fh, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: could not save summary JSON files: {e}")
        
    # Create context object
    print("\n" + "=" * 80)
    print("CREATING CONTEXT")
    print("=" * 80)

    result = {}
    tpl = DocxTemplate(template_path)
    ctx = MncContext(
        time_range=time_period_str,
        time_period=duration_period_str,
        temperature_chart=temperature_chart,
        precipitation_chart=precipitation_chart,
        total_events_chart=total_events_chart,
        total_events=total_events_recorded,
        
        foot_patrols_map=foot_patrols_map,
        no_of_foot_patrols=foot_patrol_count,
        total_foot_patrol_hours=foot_patrol_hours,
        total_foot_patrol_distance=foot_patrol_distance,
        
        vehicle_patrols_map=vehicle_patrols_map,
        no_of_vehicle_patrol=vehicle_patrol_count,
        total_vehicle_patrol_hours=vehicle_patrol_hours,
        total_vehicle_patrol_distance=vehicle_patrol_distance,
        average_vehicle_patrol_speed=average_vehicle_speed,
        
        motorbike_patrols_map=motorbike_patrols_map,
        no_of_motor_patrol=motor_patrol_count,
        total_motor_patrol_hours=motor_patrol_hours,
        total_motor_patrol_distance=motor_patrol_distance,
        average_motor_patrol_speed=average_motor_speed,
        
        patrols_coverage_map=patrols_coverage_map,
        patrol_purpose_summary_dict=patrol_purpose_summary_dict,
        patrol_effort_summary_dict=patrol_effort_summary_dict,

        night_patrols_percent= night_pct_str ,
        routine_patrols_percent= routine_pct_str ,
        joint_patrols_percent= joint_pct_str ,
        er_user= generated_by,
        generated_on=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    # Validate images if requested
    if validate_images:
        print("\nVALIDATING IMAGES:")
        image_fields = [
            'temperature_chart', 'precipitation_chart', 'total_events_chart',
            'foot_patrols_map', 'vehicle_patrols_map', 'motorbike_patrols_map',
            'patrols_coverage_map'
        ]
        
        for field_name in image_fields:
            value = getattr(ctx, field_name)
            if value and isinstance(value, str):
                p = Path(value)
                if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".gif"):
                    if p.exists() and p.is_file():
                        print(f"{field_name}: {value}")
                    else:
                        print(f"{field_name}: NOT FOUND - {value}")
                else:
                    print(f"{field_name}: Not an image path - {value}")
            else:
                print(f"{field_name}: Not provided")
    
    # Convert context to dictionary and prepare image fields
    context_dict = asdict(ctx)
    for key, value in context_dict.items():
        if isinstance(value, str) and Path(value).exists() and Path(value).suffix.lower() in (".png", ".jpg", ".jpeg"):
            result[key] = InlineImage(tpl, value, width=Cm(box_w_cm), height=Cm(box_h_cm))
        else:
            result[key] = value

    # Print full context summary
    print("\n" + "=" * 80)
    print("FINAL CONTEXT SUMMARY")
    print("=" * 80)
    for key, value in context_dict.items():
        if value is not None:
            # Truncate long values for display
            display_value = str(value)
            if len(display_value) > 100:
                display_value = display_value[:97] + "..."
            print(f"summary:{key}: {display_value}")
        else:
            print(f"err:{key}: None")
    
    # Render template
    print("\n" + "=" * 80)
    print("RENDERING DOCUMENT")
    print("=" * 80)
    
    try:
        tpl.render(result)
        tpl.save(output_path)
        print(f"\nDocument generated successfully!")
        print(f"Output: {output_path}")
        print("=" * 80)
        return str(output_path)
    except Exception as e:
        print(f"\nError rendering document: {str(e)}")
        raise