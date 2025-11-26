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
    if isinstance(df, pd.DataFrame):
        return df
    
    if df is None:
        return pd.DataFrame()

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


# for the mnc context ,the following items need to be added section by section 
# 1. weather 
# temperature_chart -> temperature_readings_over_time.png
# precipitation_chart -> precipitation_readings_over_time.png
# atmospheric_pressure_chart  -> atmospheric_pressure_readings_over_time.png
# wind_gusts_chart-> wind_gusts_readings_over_time.png
# wind_speed_chart -> wind_speed_readings_over_time.png
# soil_temperature_chart -> soil_temperature_readings_over_time.png

# 2. Patrol Effort 
# total events - int  --> total_events_recorded summary table ["no_of_events"] last column 
# total_events_chart -> total_events_recorded.png 

# foot patrol summary table 
# -- foot_patrol_efforts.csv
# no_of_foot_patrols -int,[no_of_patrols] total_foot_patrol_hours -float,[duration_hrs] 
# total_foot_patrol_distance -float[distance_km], average_speed -float[average_speed]
# foot_patrols_map - foot_patrols_map.png

# vehicle patrol summary table 
# -- vehicle_patrol_efforts.csv
# no_of_vehicle_patrols -int[ no_of_patrols ], total_vehicle_patrol_hours -float[duration_hrs], 
# total_vehicle_patrol_distance -float[distance_km] , average_speed -float [average_speed]
# vehicle_patrols_map - vehicle_patrols_map.png

# motorbike patrol summary table 
# no_of_motor_patrols -int[ no_of_patrols ], total_motor_patrol_distance -float [distance_km], 
# total_motor_patrol_hours -float [duration_hrs ], average_motor_patrol_speed -float [average_speed ]
# motorbike_patrols_map - motorbike_patrols_map.png 

# patrol coverage 
# mara_conservancy_percentage covered - float [conservancy_name]== 'Mara North Conservancy' [occupancy_percentage]
# patrols_coverage_map - patrol_coverage_map.png 
# patrol coverage summary table -patrol_coverage.csv [ patrol_coverage ]

# purpose of patrols 
# night_patrols_percent -float [purpose]night [no_of_patrols]/total , 
# routine_patrols_percent -float [purpose]routine [no_of_patrols]/total , 
# joint_patrols_percent -float [purpose]joint [no_of_patrols]/total , 
# patrol_purpose_summary_table  - patrol_purpose_summary.csv [ patrol_purpose ]

# patrol effort by ranger 
# patrol_effort_by_ranger_table - overall_patrol_efforts.csv [ patrol_efforts ]

# 3. Livestock Monitoring 
# Cattle counts 
# no_of_cow_events -int  # not done yet so pass nothing 
# table_count_summary_table - mobile_boma_summary.csv [ zone_stats ]

# Boma movements 
# no_of_boma_movements -int  mobile_boma_summary.csv [boma][Total] total_count
# boma_movements_ecomap -boma_movement_map.png 

# Livestock predation events 
# livestock_predation_events -int -- livestock_events_recorded.csv [date]Total no_of_events 

# livestock_predation_events_summary_table -- livestock_predation_events.csv
# livestock_predation_events_ecomap - livestock_predation_events.png 


# 4. Wildlife Monitoring 
# wildlife incidents 
# total_wildlife_incidents -int -wildlife_incidents_recorded.csv [date]Total [no_of_events]

# wildlife_incidents_recorded -summary_table - wildlife_incidents_summary.csv
# wildlife_incidents_events_ecomap - wildlife_incidents_map.png 

# Elephant 
# no_of_elephant_events -int -- elephant_events_recorded.csv [event_type]Total [no_of_events]
# elephant_events_distribution -bar_chart -- elephant_herd_size_bar_chart.png 
# elephant_sightings_ecomap -- elephant_sightings_events.png 
# elephant_herd_types_ecomap - elephant_herd_types_map.png 

# Buffalo 
# no_of_buffalo_events -int - buffalo_events_recorded.csv [event_type]Total [no_of_events]
# buffalo_events_distribution -bar_chart - buffalo_herd_size_bar_chart.png 
# buffalo_sightings_ecomap - buffalo_herd_map.png 
# buffalo_herd_types_ecomap - buffalo_herd_types_map.png 

# Rhino 
# no_of_rhino_events -int -rhino_events_recorded.csv[event_type]Total [no_of_events]
# rhino_events_sightings  -rhino_sighting_map.png

# Lion 
# no_of_lion_events -int -lion_events_recorded.csv[event_type]Total [no_of_events]
# common lion prides , A-B-C - unique_lion_prides.csv [ lion_pride ][no_of_events]
# lion_sightings_ecomap - lion_sightings_map

# Leopard
# no_of_leopard_sightings -int -leopard_events_recorded.csv[event_type]Total [no_of_events]
# common individuals - A-B-C - individual_leopard_summary.csv [individual_present][no_of_events]cheetah_observations
# leopard_sightings_ecomap - leopard_sightings_map.png

# Cheetah 
# no_of_cheetah_events -int -cheetah_events_recorded.csv[event_type]Total [no_of_events]
# common observed cheetah -str - individual_cheetah_summary.csv [individual_present][no_of_events]
#  individual cheetah observations -summary_table  -- individual_cheetah_summary.csv
# cheetah_sightings_ecomap -- cheetah_sightings_map.png


# 5. Logistics
# Balloon landings 
# Balloon landing summary table -- not yet available 

# Airstrip arrival and departure 
# Airstrip arrival_and_departure_summary_table - airstrip_arrivals_and_departure.csv [ airstrip_observations ]

# Airstrip maintenace 
# airstrip maintenace summary table  -- not yet available 


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

    template_path: str,
    output_dir: str,
    generated_by: Optional[str]=None,
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
    
# df['no_of_events'].iloc[-1] if 'no_of_events' in df.columns else 0

@task 
def generate_mnc_report(
    template_path: str,
    output_dir: str,
    generated_by: Optional[str] = None, 
    validate_images: bool = True,
    box_h_cm: float = 6.5,
    box_w_cm: float = 11.11,
    time_period: Optional[TimeRange] = None, 
    filename: Optional[str] = None 
) -> str:
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
    
    # Define image extensions
    IMAGE_EXTS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp'}
    
    # Scan for all images and CSVs in output_dir
    images_found = {}
    csvs_found = {}
    
    for root, _, files in os.walk(output_dir):
        for f in files:
            p = Path(root) / f
            if p.suffix.lower() in IMAGE_EXTS:
                var_name = p.stem
                images_found[var_name] = str(p)
            elif p.suffix.lower() == '.csv':
                var_name = p.stem
                csvs_found[var_name] = str(p)
    
    print(f"Found {len(images_found)} images and {len(csvs_found)} CSV files")
    
    # Load template
    tpl = DocxTemplate(template_path)
    
    # Build context dictionary
    context = {}
    
    # ==========================
    # IMAGE MAPPINGS
    # ==========================
    
    # 1. Weather images
    weather_images = {
        'temperature_chart': 'temperature_readings_over_time',
        'precipitation_chart': 'precipitation_readings_over_time',
        'atmospheric_pressure_chart': 'atmospheric_pressure_readings_over_time',
        'wind_gusts_chart': 'wind_gusts_readings_over_time',
        'wind_speed_chart': 'wind_speed_readings_over_time',
        'soil_temperature_chart': 'soil_temperature_readings_over_time'
    }
    
    # 2. Patrol effort images
    patrol_images = {
        'total_events_chart': 'total_events_recorded',
        'foot_patrols_map': 'foot_patrols_map',
        'vehicle_patrols_map': 'vehicle_patrols_map',
        'motorbike_patrols_map': 'motorbike_patrols_map',
        'patrols_coverage_map': 'patrol_coverage_map'
    }
    
    # 3. Livestock monitoring images
    livestock_images = {
        'boma_movement_ecomap': 'boma_movement_map',
        'livestock_predation_events_ecomap': 'livestock_predation_events'
    }
    
    # 4. Wildlife monitoring images
    wildlife_images = {
        'wildlife_incidents_events_ecomap': 'wildlife_incidents_map',
        'elephant_events_distribution': 'elephant_herd_size_bar_chart',
        'elephant_sighting_ecomap': 'elephant_sightings_events',
        'elephant_herd_types_ecomap': 'elephant_herd_types_map',
        'buffalo_events_distribution': 'buffalo_herd_size_bar_chart',
        'buffalo_sightings_ecomap': 'buffalo_herd_map',
        'buffalo_herd_types_ecomap': 'buffalo_herd_types_map',
        'rhino_events_sightings': 'rhino_sighting_map',
        'lion_sightings_ecomap': 'lion_sightings_map',
        'leopard_sightings_ecomap': 'leopard_sightings_map',
        'cheetah_sightings_ecomap': 'cheetah_sightings_map'
    }
    
    # Combine all image mappings
    all_image_mappings = {
        **weather_images,
        **patrol_images,
        **livestock_images,
        **wildlife_images
    }
    
    # Add images to context
    for template_var, file_stem in all_image_mappings.items():
        if file_stem in images_found:
            img_path = images_found[file_stem]
            try:
                context[template_var] = InlineImage(
                    tpl, 
                    img_path, 
                    width=Cm(box_w_cm), 
                    height=Cm(box_h_cm)
                )
            except Exception as e:
                print(f"Warning: Could not load image {template_var}: {e}")
                context[template_var] = None
        else:
            context[template_var] = None
            if validate_images:
                print(f"Warning: Image not found for {template_var} (expected: {file_stem})")
    
    # ==========================
    # TABLE MAPPINGS
    # ==========================
    
    table_mappings = {
        # Patrol effort tables
        'patrol_efforts': 'overall_patrol_efforts',
        'foot_patrol_efforts': 'foot_patrol_efforts',
        'vehicle_patrol_efforts': 'vehicle_patrol_efforts',
        'patrol_coverage': 'patrol_coverage',
        'patrol_purpose': 'patrol_purpose_summary',
        
        # Livestock tables
        'zone_stats': 'mobile_boma_summary',
        'livestock_predation_events': 'livestock_predation_events',
        
        # Wildlife tables
        'wildlife_incidents_summary': 'wildlife_incidents_summary',
        'unique_lion_prides': 'unique_lion_prides',
        'individual_leopard_summary': 'individual_leopard_summary',
        'individual_cheetah_summary': 'individual_cheetah_summary',
        'cheetah_observations': 'individual_cheetah_summary',
        
        # Logistics tables
        'airstrip_observations': 'airstrip_arrivals_and_departure'
    }
    
    # Add tables to context
    for template_var, file_stem in table_mappings.items():
        if file_stem in csvs_found:
            csv_path = csvs_found[file_stem]
            try:
                df = pd.read_csv(csv_path)
                df = df.fillna(0)
                context[template_var] = df.to_dict(orient='records')
            except Exception as e:
                print(f"Warning: Could not load CSV {template_var}: {e}")
                context[template_var] = []
        else:
            context[template_var] = []
            print(f"Info: CSV not found for {template_var} (expected: {file_stem})")
    
    # ==========================
    # EXTRACT SPECIFIC VALUES FROM CSVs
    # ==========================
    
    # Helper function to safely read CSV
    def read_csv_safe(file_stem):
        if file_stem in csvs_found:
            try:
                return pd.read_csv(csvs_found[file_stem])
            except Exception as e:
                print(f"Warning: Could not read {file_stem}: {e}")
        return None
    
    # 1. Total events
    df = read_csv_safe('total_events_recorded')
    if df is not None and 'no_of_events' in df.columns:
        context['total_events'] = int(df['no_of_events'].iloc[-1])
    else:
        context['total_events'] = 0
    
    # 2. Foot patrol summary
    df = read_csv_safe('foot_patrol_efforts')
    if df is not None:
        context['no_of_foot_patrols'] = int(df['no_of_patrols'].iloc[-1]) if 'no_of_patrols' in df.columns else 0
        context['total_foot_patrol_hours'] = round(float(df['duration_hrs'].iloc[-1]),2) if 'duration_hrs' in df.columns else 0.0
        context['total_foot_patrol_distance'] = round(float(df['distance_km'].iloc[-1]),2)if 'distance_km' in df.columns else 0.0
        context['average_foot_patrol_speed'] = round(float(df['average_speed'].iloc[-1]),2) if 'average_speed' in df.columns else 0.0
    
    # 3. Vehicle patrol summary
    df = read_csv_safe('vehicle_patrol_efforts')
    if df is not None:
        context['no_of_vehicle_patrols'] = int(df['no_of_patrols'].iloc[-1]) if 'no_of_patrols' in df.columns else 0
        context['total_vehicle_patrol_hours'] = round(float(df['duration_hrs'].iloc[-1]),2) if 'duration_hrs' in df.columns else 0.0
        context['total_vehicle_patrol_distance'] = round(float(df['distance_km'].iloc[-1]),2) if 'distance_km' in df.columns else 0.0
        context['average_vehicle_patrol_speed'] = round(float(df['average_speed'].iloc[-1]),2) if 'average_speed' in df.columns else 0.0
    
    # 4. Motorbike patrol summary
    df = read_csv_safe('motorbike_patrol_efforts')
    if df is not None:
        context['no_of_motor_patrols'] = int(df['no_of_patrols'].iloc[-1]) if 'no_of_patrols' in df.columns else 0
        context['total_motor_patrol_distance'] = round(float(df['distance_km'].iloc[-1]),2) if 'distance_km' in df.columns else 0.0
        context['total_motor_patrol_hours'] = round(float(df['duration_hrs'].iloc[-1]),2) if 'duration_hrs' in df.columns else 0.0
        context['average_motor_patrol_speed'] = round(float(df['average_speed'].iloc[-1]),2) if 'average_speed' in df.columns else 0.0
    
    # 5. Patrol coverage - Mara North Conservancy percentage
    df = read_csv_safe('patrol_coverage')
    if df is not None and 'conservancy_name' in df.columns and 'occupancy_percentage' in df.columns:
        mnc_row = df[df['conservancy_name'] == 'Mara North Conservancy']
        context['mara_conservancy_percentage'] = round(float(mnc_row['occupancy_percentage'].iloc[0]),2) if not mnc_row.empty else 0.0
    else:
        context['mara_conservancy_percentage'] = 0.0
    
    # 6. Patrol purpose percentages
    df = read_csv_safe('patrol_purpose_summary')
    if df is not None and 'purpose' in df.columns and 'no_of_patrols' in df.columns:
        total_patrols = df['no_of_patrols'].iloc[-1]
        
        night_row = df[df['purpose'] == 'night']
        context['night_patrols_percent'] = float((night_row['no_of_patrols'].iloc[0] / total_patrols * 100)) if not night_row.empty and total_patrols > 0 else 0.0
        
        routine_row = df[df['purpose'] == 'routine']
        context['routine_patrols_percent'] = float((routine_row['no_of_patrols'].iloc[0] / total_patrols * 100)) if not routine_row.empty and total_patrols > 0 else 0.0
        
        joint_row = df[df['purpose'] == 'joint']
        context['joint_patrols_percent'] = float((joint_row['no_of_patrols'].iloc[0] / total_patrols * 100)) if not joint_row.empty and total_patrols > 0 else 0.0
    else:
        context['night_patrols_percent'] = 0.0
        context['routine_patrols_percent'] = 0.0
        context['joint_patrols_percent'] = 0. # pause on this 
    
    # 7. Boma movements
    df = read_csv_safe('mobile_boma_summary')
    if df is not None and 'boma' in df.columns and 'total_count' in df.columns:
        total_row = df[df['boma'] == 'Total']
        context['no_of_boma_movements'] = int(total_row['total_count'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_boma_movements'] = 0
    
    # 8. Livestock predation events
    df = read_csv_safe('livestock_events_recorded')
    if df is not None and 'date' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['date'] == 'Total']
        context['total_livestock_predation_events'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['livestock_predation_events'] = 0
    
    # 9. Total wildlife incidents
    df = read_csv_safe('wildlife_incidents_recorded')
    if df is not None and 'date' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['date'] == 'Total']
        context['total_wildlife_incidents'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['total_wildlife_incidents'] = 0
    
    # 10. Elephant events
    df = read_csv_safe('elephant_events_recorded')
    if df is not None and 'event_type' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['event_type'] == 'Total']
        context['no_of_elephant_events'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_elephant_events'] = 0
    
    # 11. Buffalo events
    df = read_csv_safe('buffalo_events_recorded')
    if df is not None and 'event_type' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['event_type'] == 'Total']
        context['no_of_buffalo_events'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_buffalo_events'] = 0
    
    # 12. Rhino events
    df = read_csv_safe('rhino_events_recorded')
    if df is not None and 'event_type' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['event_type'] == 'Total']
        context['no_of_rhino_events'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_rhino_events'] = 0
    
    # 13. Lion events and top 3 prides
    df = read_csv_safe('lion_events_recorded')
    if df is not None and 'event_type' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['event_type'] == 'Total']
        context['no_of_lion_events'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_lion_events'] = 0
    
    df = read_csv_safe('unique_lion_prides')
    if df is not None and 'lion_pride' in df.columns and 'no_of_events' in df.columns:
        top_prides = df.nlargest(3, 'no_of_events')['lion_pride'].tolist()
        context['common_lion_prides'] = ', '.join(top_prides) if top_prides else 'N/A'
    else:
        context['common_lion_prides'] = 'N/A'

    # 14. Leopard sightings and top 3 individuals
    df = read_csv_safe('leopard_events_recorded')
    if df is not None and 'event_type' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['event_type'] == 'Total']
        context['no_of_leopard_sightings'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_leopard_sightings'] = 0
    
    df = read_csv_safe('individual_leopard_summary')
    if df is not None and 'individual_present' in df.columns and 'no_of_events' in df.columns:
        top_individuals = df.nlargest(3, 'no_of_events')['individual_present'].tolist()
        context['common_leopard_individuals'] = ', '.join(top_individuals) if top_individuals else 'N/A'
    else:
        context['common_leopard_individuals'] = 'N/A'
    
    # 15. Cheetah events and common individuals
    df = read_csv_safe('cheetah_events_recorded')
    if df is not None and 'event_type' in df.columns and 'no_of_events' in df.columns:
        total_row = df[df['event_type'] == 'Total']
        context['no_of_cheetah_events'] = int(total_row['no_of_events'].iloc[0]) if not total_row.empty else 0
    else:
        context['no_of_cheetah_events'] = 0
    
    df = read_csv_safe('individual_cheetah_summary')
    if df is not None and 'individual_present' in df.columns and 'no_of_events' in df.columns:
        top_individuals = df.nlargest(3, 'no_of_events')['individual_present'].tolist()
        context['common_cheetah_individuals'] = ', '.join(top_individuals) if top_individuals else 'N/A'
    else:
        context['common_cheetah_individuals'] = 'N/A'
    
    # ==========================
    # METADATA
    # ==========================
    
    if generated_by:
        context['er_user'] = generated_by
    
    time_period_str = None
    if time_period:
        fmt = getattr(time_period, "time_format", "%Y-%m-%d")
        time_period_str = f"{time_period.since.strftime(fmt)} to {time_period.until.strftime(fmt)}"

    context['time_range'] = time_period_str
    
    context['generated_on'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # ==========================
    # RENDER AND SAVE
    # ==========================
    
    output_filename = filename or f"MNC_Report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.docx"
    output_path = os.path.join(output_dir, output_filename)
    
    print(f"context:{context}")
    tpl.render(context)
    tpl.save(output_path)
    
    print(f"\nDocument generated successfully!")
    print(f"Output: {output_path}")
    print("=" * 80)
    return str(output_path)

