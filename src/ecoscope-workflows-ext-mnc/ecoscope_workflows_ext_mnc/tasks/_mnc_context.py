import os
import uuid
from pathlib import Path
from docx.shared import Cm
from typing import Optional
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


@task
def create_mnc_context(
    template_path: str,
    output_dir: str,
    total_events_df: AnyDataFrame,
    foot_patrols_summary_df: AnyDataFrame,
    vehicle_patrols_summary_df: AnyDataFrame,
    motor_patrols_summary_df: AnyDataFrame,
    patrol_purpose_summary_df: AnyDataFrame,
    patrol_effort_summary_df: AnyDataFrame,
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
    print(f"\n📁 Template Path: {template_path}")
    print(f"📁 Output Directory: {output_dir}")
    
    # Validate paths
    if not template_path.strip():
        raise ValueError("template_path is empty after normalization")
    if not output_dir.strip():
        raise ValueError("output_directory is empty after normalization")
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    if not filename:
        filename = f"mnc_report_{uuid.uuid4().hex[:8]}.docx"
    output_path = Path(output_dir) / filename
    print(f"📄 Output File: {output_path}")
    
    # Process time period
    time_period_str = None
    duration_period_str = None
    if time_period:
        fmt = getattr(time_period, "time_format", "%Y-%m-%d")
        time_period_str = f"{time_period.since.strftime(fmt)} to {time_period.until.strftime(fmt)}"
        # Duration period - dates only
        date_fmt = "%Y-%m-%d"
        duration_period_str = f"{time_period.since.strftime(date_fmt)} to {time_period.until.strftime(date_fmt)}"
        print(f"\n📅 Time Range: {time_period_str}")
        print(f"📅 Duration Period: {duration_period_str}")
    
    # Extract total events
    print("\n" + "=" * 80)
    print("PROCESSING DATA")
    print("=" * 80)
    
    # Total events - handle both single value and DataFrame
    if hasattr(total_events_df, 'iloc'):
        total_events_recorded = str(total_events_df.iloc[0, 0]) if not total_events_df.empty else "0"
    else:
        total_events_recorded = str(total_events_df)
    print(f"\n📊 Total Events: {total_events_recorded}")
    
    # Extract foot patrol data
    print("\n🚶 FOOT PATROLS:")
    foot_patrol_count = str(foot_patrols_summary_df.get("no_of_patrols", 0))
    foot_patrol_hours = str(round(float(foot_patrols_summary_df.get("duration_hrs", 0)), 2))
    foot_patrol_distance = str(round(float(foot_patrols_summary_df.get("distance_km", 0)), 2))
    print(f"  • Count: {foot_patrol_count}")
    print(f"  • Hours: {foot_patrol_hours}")
    print(f"  • Distance: {foot_patrol_distance} km")
    
    # Extract vehicle patrol data
    print("\n🚗 VEHICLE PATROLS:")
    vehicle_patrol_count = str(vehicle_patrols_summary_df.get("no_of_patrols", 0))
    vehicle_patrol_hours = str(round(float(vehicle_patrols_summary_df.get("duration_hrs", 0)), 2))
    vehicle_patrol_distance = str(round(float(vehicle_patrols_summary_df.get("distance_km", 0)), 2))
    average_vehicle_speed = str(round(float(vehicle_patrols_summary_df.get("average_speed", 0)), 2))
    
    print(f"  • Count: {vehicle_patrol_count}")
    print(f"  • Hours: {vehicle_patrol_hours}")
    print(f"  • Distance: {vehicle_patrol_distance} km")
    print(f"  • Avg Speed: {average_vehicle_speed} km/h")
    
    # Extract motorbike patrol data
    print("\n🏍️ MOTORBIKE PATROLS:")
    motor_patrol_count = str(motor_patrols_summary_df.get("no_of_patrols", 0))
    motor_patrol_hours = str(round(float(motor_patrols_summary_df.get("duration_hrs", 0)), 2))
    motor_patrol_distance = str(round(float(motor_patrols_summary_df.get("distance_km", 0)), 2))
    average_motor_speed = str(round(float(motor_patrols_summary_df.get("average_speed", 0)), 2))
    
    print(f"  • Count: {motor_patrol_count}")
    print(f"  • Hours: {motor_patrol_hours}")
    print(f"  • Distance: {motor_patrol_distance} km")
    print(f"  • Avg Speed: {average_motor_speed} km/h")
    
    # Convert summary DataFrames to dictionaries
    print("\n📋 PATROL SUMMARIES:")
    patrol_purpose_summary_dict = patrol_purpose_summary_df.to_dict('records') if hasattr(patrol_purpose_summary_df, 'to_dict') else {}
    patrol_effort_summary_dict = patrol_effort_summary_df.to_dict('records') if hasattr(patrol_effort_summary_df, 'to_dict') else {}
    print(f"  • Purpose Summary: {len(patrol_purpose_summary_dict) if isinstance(patrol_purpose_summary_dict, list) else 'N/A'} records")
    print(f"  • Effort Summary: {len(patrol_effort_summary_dict) if isinstance(patrol_effort_summary_dict, list) else 'N/A'} records")
    
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
        patrol_effort_summary_dict=patrol_effort_summary_dict
    )
    
    # Validate images if requested
    if validate_images:
        print("\n🖼️ VALIDATING IMAGES:")
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
                        print(f"  ✅ {field_name}: {value}")
                    else:
                        print(f"  ❌ {field_name}: NOT FOUND - {value}")
                else:
                    print(f"  ⚠️ {field_name}: Not an image path - {value}")
            else:
                print(f"  ⏭️ {field_name}: Not provided")
    
    # Convert context to dictionary
    context_dict = asdict(ctx)
    for key, value in context_dict.items():
        if isinstance(value, str) and Path(value).suffix.lower() in (".png", ".jpg", ".jpeg"):
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
            print(f"  ✓ {key}: {display_value}")
        else:
            print(f"  ✗ {key}: None")
    
    # Render template
    print("\n" + "=" * 80)
    print("RENDERING DOCUMENT")
    print("=" * 80)
    print(f"\n✅ Document generated successfully!")
    print(f"📄 Output: {output_path}")
    print("=" * 80)
    tpl.render(result)
    tpl.save(output_path)
    return str(output_path)
