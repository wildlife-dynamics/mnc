import os
import pandas as pd
from pathlib import Path
from docx.shared import Cm
from typing import Optional
from docxtpl import DocxTemplate, InlineImage
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.tasks.filter._filter import TimeRange
from ecoscope_workflows_ext_custom.tasks.io._path_utils import remove_file_scheme


@task
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
                var_name = p.stem
                images_found[var_name] = str(p)
            elif p.suffix.lower() == ".csv":
                var_name = p.stem
                csvs_found[var_name] = str(p)

    print(f"Found {len(images_found)} images and {len(csvs_found)} CSV files")
    # Load template
    tpl = DocxTemplate(template_path)

    # Build context dictionary
    context = {}

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
        "foot_patrols_map": "foot_patrols_map",
        "vehicle_patrols_map": "vehicle_patrols_map",
        "motorbike_patrols_map": "motorbike_patrols_map",
        "patrols_coverage_map": "patrol_coverage_map",
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
        "rhino_events_sightings": "rhino_sighting_events",
        "lion_sightings_ecomap": "lion_sightings_events",
        "leopard_sightings_ecomap": "leopard_sightings_events",
        "cheetah_sightings_ecomap": "cheetah_sightings_events",
        "giraffe_events_sightings": "giraffe_sightings_events",
        "hartebeest_events_sightings": "hartebeest_sightings_events",
    }

    # Combine all image mappings
    all_image_mappings = {**weather_images, **patrol_images, **livestock_images, **wildlife_images}

    # Add images to context
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
        "lion_events_recorded": "total_lion_events_recorded",
        "individual_lions_summary": "individual_lions_summary",
        "individual_leopard_summary": "individual_leopard_summary",
        "individual_cheetah_summary": "individual_cheetah_summary",
        "leopard_events_recorded": "total_leopard_events_recorded",
        "cheetah_events_recorded": "total_cheetah_events_recorded",
        "elephant_events_recorded": "total_elephants_events_recorded",
        "total_events_recorded": "total_events_recorded_by_date",
        "buffalo_events_recorded": "total_buffalo_events_recorded",
        "rhino_events_recorded": "total_rhino_events_recorded",
        # Logistics tables
        "airstrip_observations": "airstrip_operations_summary_table",
        "balloon_observations": "balloon_landing_summary_table",
        "airstrip_maintenance_observations": "airstrip_maintenance_summary_table",
    }
    # Add tables to context
    for template_var, file_stem in table_mappings.items():
        if file_stem in csvs_found:
            csv_path = csvs_found[file_stem]
            try:
                df = pd.read_csv(csv_path)
                df = df.fillna(0)
                context[template_var] = df.to_dict(orient="records")
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

    # Patrol efforts processing
    patrol_efforts_df = read_csv_safe("overall_patrol_efforts")
    if patrol_efforts_df is not None:
        patrol_efforts_df["no_of_patrols"] = patrol_efforts_df["no_of_patrols"].fillna(0).astype(int)
        patrol_efforts_df["distance_km"] = patrol_efforts_df["distance_km"].round(2)
        patrol_efforts_df["duration_hrs"] = patrol_efforts_df["duration_hrs"].round(2)
        context["patrol_efforts"] = patrol_efforts_df.to_dict(orient="records")

    # Airstrip processing - overwrites the key from table_mappings
    air_df = read_csv_safe("airstrip_operations_summary_table")
    if air_df is not None:
        air_df = air_df.rename(columns={"Arrival": "arrival", "Departure": "departure"})
        air_df["arrival"] = air_df["arrival"].fillna(0).astype(int)
        air_df["departure"] = air_df["departure"].fillna(0).astype(int)
        context["airstrip_observations"] = air_df.to_dict(orient="records")

    df = read_csv_safe("total_events_recorded_by_date")
    if df is not None and "no_of_events" in df.columns:
        context["total_events"] = int(df["no_of_events"].iloc[-1])
    else:
        context["total_events"] = 0

    # Foot patrol summary
    df = read_csv_safe("foot_patrol_efforts")
    if df is not None:
        context["no_of_foot_patrols"] = int(df["no_of_patrols"].iloc[-1]) if "no_of_patrols" in df.columns else 0
        context["total_foot_patrol_hours"] = (
            round(float(df["duration_hrs"].iloc[-1]), 2) if "duration_hrs" in df.columns else 0.0
        )
        context["total_foot_patrol_distance"] = (
            round(float(df["distance_km"].iloc[-1]), 2) if "distance_km" in df.columns else 0.0
        )
        context["average_foot_patrol_speed"] = (
            round(float(df["average_speed"].iloc[-1]), 2) if "average_speed" in df.columns else 0.0
        )

    # Vehicle patrol summary
    df = read_csv_safe("vehicle_patrol_efforts")
    if df is not None:
        context["no_of_vehicle_patrols"] = int(df["no_of_patrols"].iloc[-1]) if "no_of_patrols" in df.columns else 0
        context["total_vehicle_patrol_hours"] = (
            round(float(df["duration_hrs"].iloc[-1]), 2) if "duration_hrs" in df.columns else 0.0
        )
        context["total_vehicle_patrol_distance"] = (
            round(float(df["distance_km"].iloc[-1]), 2) if "distance_km" in df.columns else 0.0
        )
        context["average_vehicle_patrol_speed"] = (
            round(float(df["average_speed"].iloc[-1]), 2) if "average_speed" in df.columns else 0.0
        )

    # Motorbike patrol summary
    df = read_csv_safe("motorbike_patrol_efforts")
    if df is not None:
        context["no_of_motor_patrols"] = int(df["no_of_patrols"].iloc[-1]) if "no_of_patrols" in df.columns else 0
        context["total_motor_patrol_distance"] = (
            round(float(df["distance_km"].iloc[-1]), 2) if "distance_km" in df.columns else 0.0
        )
        context["total_motor_patrol_hours"] = (
            round(float(df["duration_hrs"].iloc[-1]), 2) if "duration_hrs" in df.columns else 0.0
        )
        context["average_motor_patrol_speed"] = (
            round(float(df["average_speed"].iloc[-1]), 2) if "average_speed" in df.columns else 0.0
        )

    # Patrol coverage - Mara North Conservancy percentage
    df = read_csv_safe("patrol_coverage")
    if df is not None and "conservancy_name" in df.columns and "occupancy_percentage" in df.columns:
        mnc_row = df[df["conservancy_name"] == "Mara North Conservancy"]
        context["mara_conservancy_percentage"] = (
            round(float(mnc_row["occupancy_percentage"].iloc[0]), 2) if not mnc_row.empty else 0.0
        )
    else:
        context["mara_conservancy_percentage"] = 0.0

    # Patrol purpose percentages
    df = read_csv_safe("patrol_purpose_summary")
    if df is not None and "purpose" in df.columns and "no_of_patrols" in df.columns:
        total_patrols = df["no_of_patrols"].iloc[-1]

        night_row = df[df["purpose"] == "night"]
        context["night_patrols_percent"] = (
            float((night_row["no_of_patrols"].iloc[0] / total_patrols * 100))
            if not night_row.empty and total_patrols > 0
            else 0.0
        )

        routine_row = df[df["purpose"] == "routine"]
        context["routine_patrols_percent"] = (
            float((routine_row["no_of_patrols"].iloc[0] / total_patrols * 100))
            if not routine_row.empty and total_patrols > 0
            else 0.0
        )

        joint_row = df[df["purpose"] == "joint"]
        context["joint_patrols_percent"] = (
            float((joint_row["no_of_patrols"].iloc[0] / total_patrols * 100))
            if not joint_row.empty and total_patrols > 0
            else 0.0
        )
    else:
        context["night_patrols_percent"] = 0.0
        context["routine_patrols_percent"] = 0.0
        context["joint_patrols_percent"] = 0.0

    # Boma movements
    df = read_csv_safe("mobile_boma_movement_summary_table")
    if df is not None and "date" in df.columns and "boma_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_boma_movements"] = int(total_row["boma_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_boma_movements"] = 0

    # Livestock predation events
    df = read_csv_safe("livestock_predation_summary_table")
    if df is not None and "date" in df.columns:
        total_events = int(df["total_livestock_affected"].count())
        context["total_livestock_predation_events"] = total_events
    else:
        context["total_livestock_predation_events"] = 0

    # Total wildlife incidents
    df = read_csv_safe("wildlife_incidents_summary_table")
    if df is not None and "event_type" in df.columns:
        total_events = int(df["records"].sum())
        context["total_wildlife_incidents"] = total_events
    else:
        context["total_wildlife_incidents"] = 0

    # Elephant events
    df = read_csv_safe("total_elephants_events_recorded")
    if df is not None and "date" in df.columns and "no_of_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_elephant_events"] = int(total_row["no_of_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_elephant_events"] = 0

    # Buffalo events
    df = read_csv_safe("total_buffalo_events_recorded")
    if df is not None and "date" in df.columns and "no_of_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_buffalo_sightings"] = int(total_row["no_of_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_buffalo_sightings"] = 0

    # Rhino events
    df = read_csv_safe("total_rhino_events_recorded")
    if df is not None and "date" in df.columns and "no_of_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_rhino_events"] = int(total_row["no_of_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_rhino_events"] = 0

    # Lion events and top 3 prides
    df = read_csv_safe("total_lion_events_recorded")
    if df is not None and "date" in df.columns and "no_of_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_lion_events"] = int(total_row["no_of_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_lion_events"] = 0

    df = read_csv_safe("individual_lions_summary")
    if df is not None and "pride" in df.columns and "no_of_events" in df.columns:
        top_prides = df.nlargest(3, "no_of_events")["pride"].tolist()
        context["common_lion_prides"] = ", ".join(top_prides) if top_prides else "N/A"
    else:
        context["common_lion_prides"] = "N/A"

    # Leopard sightings and top 3 individuals
    df = read_csv_safe("total_leopard_events_recorded")
    if df is not None and "date" in df.columns and "no_of_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_leopard_sightings"] = int(total_row["no_of_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_leopard_sightings"] = 0

    df = read_csv_safe("individual_leopard_summary")
    if df is not None and "individuals_present" in df.columns and "no_of_events" in df.columns:
        top_individuals = df.nlargest(3, "no_of_events")["individuals_present"].tolist()
        context["common_leopard_individuals"] = ", ".join(top_individuals) if top_individuals else "N/A"
    else:
        context["common_leopard_individuals"] = "N/A"

    # Cheetah events and common individuals
    df = read_csv_safe("total_cheetah_events_recorded")
    if df is not None and "date" in df.columns and "no_of_events" in df.columns:
        total_row = df[df["date"] == "Total"]
        context["no_of_cheetah_events"] = int(total_row["no_of_events"].iloc[0]) if not total_row.empty else 0
    else:
        context["no_of_cheetah_events"] = 0

    df = read_csv_safe("individual_cheetah_summary")
    if df is not None and "individuals_present" in df.columns and "no_of_events" in df.columns:
        df = df.sort_values(by="no_of_events", ascending=False)
        top_individuals = df.nlargest(3, "no_of_events")["individuals_present"].tolist()
        context["common_cheetah_individuals"] = ", ".join(top_individuals) if top_individuals else "N/A"
    else:
        context["common_cheetah_individuals"] = "N/A"
    context["individual_cheetah_summary"] = df.to_dict(orient="records")

    df = read_csv_safe("total_cattle_count_summary_table")
    if df is not None and "date" in df.columns:
        total_events = int(df["date"].count())
        context["no_of_cow_events"] = total_events
    else:
        context["no_of_cow_events"] = 0

    # ==========================
    # METADATA
    # ==========================

    if generated_by:
        context["er_user"] = generated_by

    time_period_str = None
    if time_period:
        fmt = getattr(time_period, "time_format", "%Y-%m-%d")
        time_period_str = f"{time_period.since.strftime(fmt)} to {time_period.until.strftime(fmt)}"

    context["time_range"] = time_period_str
    context["time_period"] = f"{time_period.since.date()} - {time_period.until.date()}"
    context["generated_on"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # ==========================
    # RENDER AND SAVE
    # ==========================
    output_filename = filename or f"Overall_Report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.docx"
    output_path = os.path.join(output_dir, output_filename)
    tpl.render(context)
    tpl.save(output_path)
    print("\nDocument generated successfully!")
    print(f"Output: {output_path}")
    return str(output_path)
