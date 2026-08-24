import os
import pandas as pd
from pathlib import Path
from docx.shared import Cm
from typing import Optional
from wt_registry import register
from docxtpl import DocxTemplate, InlineImage
from ecoscope.platform.tasks.filter._filter import TimeRange
from ecoscope_workflows_ext_custom.tasks.io._path_utils import remove_file_scheme


def _read_csv_safe(csvs_found: dict, file_stem: str) -> Optional[pd.DataFrame]:
    """Return a DataFrame for the given file stem, or None if missing/unreadable."""
    if file_stem not in csvs_found:
        return None
    try:
        return pd.read_csv(csvs_found[file_stem])
    except Exception as e:
        print(f"Warning: Could not read {file_stem}: {e}")
        return None


def _has_cols(df: Optional[pd.DataFrame], *cols: str) -> bool:
    """True only if df is a non-empty DataFrame containing every named column."""
    if df is None or df.empty:
        return False
    return all(c in df.columns for c in cols)


def _safe_int(value, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float = 0.0, ndigits: int = 2) -> float:
    try:
        if pd.isna(value):
            return default
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return default


def _last(df: pd.DataFrame, col: str):
    """Last value in a column, or None if missing/empty."""
    if not _has_cols(df, col):
        return None
    s = df[col].dropna()
    return s.iloc[-1] if not s.empty else None


def _total_row_value(df: pd.DataFrame, value_col: str, date_col: str = "date", total_label: str = "Total"):
    """Value from the row where date_col == 'Total', or None."""
    if not _has_cols(df, date_col, value_col):
        return None
    total_row = df[df[date_col] == total_label]
    if total_row.empty:
        return None
    return total_row[value_col].iloc[0]


@register()
def generate_mnc_report(
    template_path: str,
    output_dir: str,
    generated_by: Optional[str] = None,
    validate_images: bool = True,
    time_period: Optional[TimeRange] = None,
    filename: Optional[str] = None,
) -> str:
    template_path = remove_file_scheme(template_path)
    output_dir = remove_file_scheme(output_dir)

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
    IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp"}
    # Scan for all images and CSVs in output_dir
    images_found = {}
    csvs_found = {}

    for root, _, files in os.walk(output_dir):
        for f in files:
            p = Path(root) / f
            if p.suffix.lower() in IMAGE_EXTS:
                images_found[p.stem] = str(p)
            elif p.suffix.lower() == ".csv":
                csvs_found[p.stem] = str(p)

    print(f"Found {len(images_found)} images and {len(csvs_found)} CSV files")
    tpl = DocxTemplate(template_path)

    context = {}

    # IMAGES
    weather_images = {
        "temperature_chart": "temperature_readings_over_time",
        "precipitation_chart": "precipitation_readings_over_time",
        "atmospheric_pressure_chart": "atmospheric_pressure_readings_over_time",
        "wind_gusts_chart": "wind_gusts_readings_over_time",
        "wind_speed_chart": "wind_speed_readings_over_time",
        "soil_temperature_chart": "soil_temperature_readings_over_time",
        "relative_humidity_chart": "relative_humidity_readings_over_time",
    }
    patrol_images = {
        "total_events_chart": "total_events_recorded",
        "foot_patrols_map": "foot_patrol_map",
        "vehicle_patrols_map": "vehicle_patrol_map",
        "motorbike_patrols_map": "motor_patrol_map",
        "patrols_coverage_map": "overall_patrol_map",
    }
    livestock_images = {
        "boma_movement_ecomap": "boma_movement_map",
        "livestock_predation_events_ecomap": "livestock_predation_events",
        "non_compliant_grazing_ecomap": "illegal_grazing_map",
    }
    wildlife_images = {
        "wildlife_incident_events_ecomap": "wildlife_incidents_map",
        "elephant_events_distribution": "elephant_herd_size_bar_chart",
        "elephant_sighting_ecomap": "elephant_sightings_events",
        "elephant_herd_types_ecomap": "elephant_herd_types_map",
        "buffalo_events_distribution": "buffalo_herd_size_bar_chart",
        "buffalo_sightings_ecomap": "buffalo_sightings_events",
        "buffalo_herd_types_ecomap": "buffalo_herd_types_map",
        "rhino_events_sightings": "rhino_sightings_map",
        "lion_sightings_ecomap": "lion_pride_sightings_map",
        "leopard_sightings_ecomap": "leopard_sightings_map",
        "cheetah_sightings_ecomap": "cheetah_sightings_map",
        "giraffe_events_sightings": "giraffe_sightings_map",
        "hartebeest_events_sightings": "hartebeest_sightings_map",
    }

    all_image_mappings = {**weather_images, **patrol_images, **livestock_images, **wildlife_images}

    for template_var, file_stem in all_image_mappings.items():
        if file_stem in images_found:
            img_path = images_found[file_stem]
            try:
                context[template_var] = InlineImage(tpl, img_path, width=Cm(14.27), height=Cm(8.35))
            except Exception as e:
                print(f"Warning: Could not load image {template_var}: {e}")
                context[template_var] = None
        else:
            context[template_var] = None
            if validate_images:
                print(f"Warning: Image not found for {template_var} (expected: {file_stem})")

    # TABLES (CSV -> list of dicts)
    table_mappings = {
        # Patrol effort tables
        "patrol_efforts": "overall_patrol_efforts",
        "foot_patrol_efforts": "foot_patrol_efforts",
        "vehicle_patrol_efforts": "vehicle_patrol_efforts",
        "patrol_coverage": "patrol_coverage",
        "patrol_purpose": "patrol_purpose_summary",
        # Livestock tables
        "zone_stats": "total_cattle_count_summary_table",
        "livestock_predation_events": "livestock_predation_summary_table",
        # Wildlife tables
        "wildlife_incidents_summary": "wildlife_incidents_summary_table",
        # "lion_events_recorded": "total_lion_events_recorded",
        "individual_lions_summary": "overall_lion_summary_table",
        "individual_leopard_summary": "overall_leopard_summary_table",
        "individual_cheetah_summary": "overall_cheetah_summary_table",
        # "leopard_events_recorded": "total_leopard_events_recorded",
        "cheetah_events_recorded": "total_cheetah_events_recorded",
        "elephant_events_recorded": "overall_elephant_summary_table",
        "total_events_recorded": "total_events_recorded_by_date",
        "buffalo_events_recorded": "overall_buffalo_summary_table",
        "rhino_events_recorded": "total_rhino_events_recorded",
        # Logistics tables
        "airstrip_observations": "airstrip_operations_summary_table",
        "balloon_observations": "balloon_landing_summary_table",
        "airstrip_maintenance_observations": "airstrip_maintenance_summary_table",
    }

    for template_var, file_stem in table_mappings.items():
        df = _read_csv_safe(csvs_found, file_stem)
        if df is None:
            context[template_var] = []
            print(f"Info: CSV not found for {template_var} (expected: {file_stem})")
            continue
        try:
            df = df.fillna(0)
            context[template_var] = df.to_dict(orient="records")
        except Exception as e:
            print(f"Warning: Could not load CSV {template_var}: {e}")
            context[template_var] = []

    # EXTRACT SPECIFIC VALUES FROM CSVs
    df = _read_csv_safe(csvs_found, "overall_patrol_efforts")
    if df is not None and not df.empty:
        if "no_of_patrols" in df.columns:
            df["no_of_patrols"] = df["no_of_patrols"].fillna(0).astype(int)
        if "distance_km" in df.columns:
            df["distance_km"] = df["distance_km"].round(2)
        if "duration_hrs" in df.columns:
            df["duration_hrs"] = df["duration_hrs"].round(2)
        context["patrol_efforts"] = df.to_dict(orient="records")

    # Airstrip operations
    air_df = _read_csv_safe(csvs_found, "airstrip_operations_summary_table")
    if air_df is not None and not air_df.empty:
        air_df = air_df.rename(columns={"Arrival": "arrival", "Departure": "departure", "Camp Lodge": "camp_lodge"})
        if "arrival" in air_df.columns:
            air_df["arrival"] = air_df["arrival"].fillna(0).astype(int)
        if "departure" in air_df.columns:
            air_df["departure"] = air_df["departure"].fillna(0).astype(int)
        context["airstrip_observations"] = air_df.to_dict(orient="records")

    df = _read_csv_safe(csvs_found, "total_events_recorded_by_date")
    context["total_events"] = _safe_int(_last(df, "no_of_events")) if df is not None else 0

    df = _read_csv_safe(csvs_found, "foot_patrol_efforts")
    context["no_of_foot_patrols"] = _safe_int(df["no_of_patrols"].sum()) if df is not None else 0
    context["total_foot_patrol_hours"] = _safe_float(df["duration_hrs"].sum()) if df is not None else 0.0
    context["total_foot_patrol_distance"] = _safe_float(df["distance_km"].sum()) if df is not None else 0.0
    context["average_foot_patrol_speed"] = _safe_float(df["average_speed"].sum()) if df is not None else 0.0

    df = _read_csv_safe(csvs_found, "vehicle_patrol_efforts")
    context["no_of_vehicle_patrols"] = _safe_int(df["no_of_patrols"].sum()) if df is not None else 0
    context["total_vehicle_patrol_hours"] = _safe_float(df["duration_hrs"].sum()) if df is not None else 0.0
    context["total_vehicle_patrol_distance"] = _safe_float(df["distance_km"].sum()) if df is not None else 0.0
    context["average_vehicle_patrol_speed"] = _safe_float(df["average_speed"].sum()) if df is not None else 0.0

    df = _read_csv_safe(csvs_found, "motorbike_patrol_efforts")
    context["no_of_motor_patrols"] = _safe_int(df["no_of_patrols"].sum()) if df is not None else 0
    context["total_motor_patrol_hours"] = _safe_float(df["duration_hrs"].sum()) if df is not None else 0.0
    context["total_motor_patrol_distance"] = _safe_float(df["distance_km"].sum()) if df is not None else 0.0
    context["average_motor_patrol_speed"] = _safe_float(df["average_speed"].sum()) if df is not None else 0.0

    # Patrol coverage - Mara North Conservancy percentage
    context["mara_conservancy_percentage"] = 0.0
    df = _read_csv_safe(csvs_found, "patrol_coverage")
    if _has_cols(df, "conservancy_name", "occupancy_percentage"):
        mnc_row = df[df["conservancy_name"] == "Mara North Conservancy"]
        if not mnc_row.empty:
            context["mara_conservancy_percentage"] = _safe_float(mnc_row["occupancy_percentage"].iloc[0])

    # Patrol purpose percentages
    context["night_patrols_percent"] = 0.0
    context["routine_patrols_percent"] = 0.0
    context["joint_patrols_percent"] = 0.0
    df = _read_csv_safe(csvs_found, "patrol_purpose_summary")
    if _has_cols(df, "purpose", "no_of_patrols"):
        total_patrols = _safe_float(_last(df, "no_of_patrols"), default=0.0, ndigits=4)
        if total_patrols > 0:
            for key, label in (
                ("night_patrols_percent", "night"),
                ("routine_patrols_percent", "routine"),
                ("joint_patrols_percent", "joint"),
            ):
                row = df[df["purpose"] == label]
                if not row.empty:
                    context[key] = float(row["no_of_patrols"].iloc[0]) / total_patrols * 100

    df = _read_csv_safe(csvs_found, "mobile_boma_movement_summary_table")
    context["no_of_boma_movements"] = _safe_int(df["boma_events"].sum()) if df is not None else 0

    # Livestock predation events
    context["total_livestock_predation_events"] = 0
    df = _read_csv_safe(csvs_found, "livestock_predation_summary_table")
    if _has_cols(df, "date", "total_livestock_affected"):
        context["total_livestock_predation_events"] = int(df["total_livestock_affected"].count())

    # Total wildlife incidents
    context["total_wildlife_incidents"] = 0
    df = _read_csv_safe(csvs_found, "wildlife_incidents_summary_table")
    if _has_cols(df, "event_type", "records"):
        try:
            context["total_wildlife_incidents"] = int(df["records"].fillna(0).sum())
        except Exception as e:
            print(f"Warning: Could not sum wildlife incidents: {e}")

    # Elephant events
    df = _read_csv_safe(csvs_found, "overall_elephant_summary_table")
    context["no_of_elephant_events"] = _safe_int(df["observations"].sum()) if df is not None else 0

    # Buffalo events
    df = _read_csv_safe(csvs_found, "overall_buffalo_summary_table")
    context["no_of_buffalo_sightings"] = _safe_int(df["observations"].sum()) if df is not None else 0

    # Rhino events
    df = _read_csv_safe(csvs_found, "overall_rhino_summary_table")
    context["no_of_rhino_events"] = _safe_int(df["observations"].sum()) if df is not None else 0

    df = _read_csv_safe(csvs_found, "overall_lion_summary_table")
    context["no_of_lion_events"] = _safe_int(df["observations"].sum()) if df is not None else 0

    context["common_lion_prides"] = "N/A"
    if df is not None:
        df = df.rename(columns={"Pride": "pride"})
        if _has_cols(df, "pride", "observations"):
            top_prides = df.nlargest(3, "observations")["pride"].dropna().astype(str).tolist()
            if top_prides:
                context["common_lion_prides"] = ", ".join(top_prides)

    df = _read_csv_safe(csvs_found, "overall_leopard_summary_table")
    context["no_of_leopard_sightings"] = _safe_int(df["observations"].sum()) if df is not None else 0

    context["common_leopard_individuals"] = "N/A"
    if df is not None:
        df = df.rename(columns={"Individuals": "individuals"})
        if _has_cols(df, "individuals", "observations"):
            top = df.nlargest(3, "observations")["individuals"].dropna().astype(str).tolist()
            if top:
                context["common_leopard_individuals"] = ", ".join(top)

    # Cheetah events + common individuals
    df = _read_csv_safe(csvs_found, "overall_cheetah_summary_table")
    context["no_of_cheetah_events"] = _safe_int(df["observations"].sum()) if df is not None else 0

    context["common_cheetah_individuals"] = "N/A"

    if df is not None:
        df = df.rename(columns={"Individuals": "individuals"})
        if _has_cols(df, "individuals", "observations"):
            cheetah_df = df.sort_values(by="observations", ascending=False)
            top = cheetah_df.nlargest(3, "observations")["individuals"].dropna().astype(str).tolist()
            if top:
                context["common_cheetah_individuals"] = ", ".join(top)
            context["individual_cheetah_summary"] = cheetah_df.fillna(0).to_dict(orient="records")
        elif not df.empty:
            context["individual_cheetah_summary"] = df.fillna(0).to_dict(orient="records")

    # Cattle / cow events
    context["no_of_cow_events"] = 0
    context["zone_stats"] = []
    df = _read_csv_safe(csvs_found, "total_cattle_count_summary_table")
    if df is not None:
        df = df.rename(
            columns={"Date": "date", "Total": "total", "Zone 1": "zone_1", "Zone 2/3": "zone_2_3", "Zone 4": "zone_4"}
        )
        if _has_cols(df, "date"):
            context["no_of_cow_events"] = int(df["date"].count())
        context["zone_stats"] = df.to_dict(orient="records")

    context["balloon_observations"] = []
    df = _read_csv_safe(csvs_found, "balloon_landing_summary_table")
    if df is not None:
        df = df.rename(
            columns={
                "Date": "date",
                "Balloon Company": "balloon_company",
                "Where Are Clients Staying": "where_are_clients_staying",
                "No Of Passengers": "no_of_passengers",
            }
        )
        df = df.fillna({"balloon_company": "Undefined", "where_are_clients_staying": "Undefined"})
        context["balloon_observations"] = df.to_dict(orient="records")

    context["airstrip_maintenance_observations"] = []
    df = _read_csv_safe(csvs_found, "airstrip_maintenance_summary_table")
    if df is not None:
        df = df.rename(columns={"Date": "date", "Maintenance Type": "activity"})
        context["airstrip_maintenance_observations"] = df.to_dict(orient="records")

    if generated_by:
        context["er_user"] = generated_by

    time_period_str = None
    time_period_short = None
    if time_period is not None:
        try:
            fmt = getattr(time_period, "time_format", "%Y-%m-%d")
            time_period_str = f"{time_period.since.strftime(fmt)} to {time_period.until.strftime(fmt)}"
            time_period_short = f"{time_period.since.date()} - {time_period.until.date()}"
        except Exception as e:
            print(f"Warning: Could not format time_period: {e}")

    context["time_range"] = time_period_str
    context["time_period"] = time_period_short
    context["generated_on"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    output_filename = filename or f"overall_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.docx"
    output_path = os.path.join(output_dir, output_filename)
    tpl.render(context)
    tpl.save(output_path)
    print("\nDocument generated successfully!")
    print(f"Output: {output_path}")
    return str(output_path)
