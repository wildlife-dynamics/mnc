from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ecoscope_workflows_ext_mnc.tasks.reporting._overall_report import generate_mnc_report


class FakeTimeRange:
    """Duck-typed stand-in for ecoscope.platform.tasks.filter._filter.TimeRange."""

    def __init__(self, since, until, time_format="%Y-%m-%d"):
        self.since = since
        self.until = until
        self.time_format = time_format


@pytest.fixture
def template_path(tmp_path):
    """A placeholder template file; DocxTemplate itself is mocked out."""
    template_file = tmp_path / "template.docx"
    template_file.touch()
    return str(template_file)


@pytest.fixture
def output_dir(tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    return out


def write_csv(output_dir, stem, df):
    df.to_csv(Path(output_dir) / f"{stem}.csv", index=False)


def write_baseline_optional_csvs(output_dir):
    """Populate the handful of optional CSVs whose code paths call
    ``.rename()`` on the result of ``_read_csv_safe`` with no ``None``
    guard (lion/leopard/cheetah/cattle/balloon/airstrip-maintenance).
    Without these present, generate_mnc_report raises AttributeError
    before it ever reaches DocxTemplate.render — see
    TestGenerateMncReportKnownFragility. Tests that exercise other parts
    of the context need this baseline just to get the function to
    complete.
    """
    write_csv(output_dir, "overall_lion_summary_table", pd.DataFrame({"observations": [5], "Pride": ["Some Pride"]}))
    write_csv(output_dir, "overall_leopard_summary_table", pd.DataFrame({"observations": [3], "Individuals": ["Leo"]}))
    write_csv(
        output_dir, "overall_cheetah_summary_table", pd.DataFrame({"observations": [2], "Individuals": ["Chester"]})
    )
    write_csv(
        output_dir,
        "total_cattle_count_summary_table",
        pd.DataFrame({"Date": ["2024-01-01"], "Total": [10], "Zone 1": [5], "Zone 2/3": [3], "Zone 4": [2]}),
    )
    write_csv(
        output_dir,
        "balloon_landing_summary_table",
        pd.DataFrame(
            {
                "Date": ["2024-01-01"],
                "Balloon Company": ["Acme"],
                "Where Are Clients Staying": ["Camp"],
                "No Of Passengers": [4],
            }
        ),
    )
    write_csv(
        output_dir,
        "airstrip_maintenance_summary_table",
        pd.DataFrame({"Date": ["2024-01-01"], "Maintenance Type": ["Grading"]}),
    )


@pytest.fixture
def full_output_dir(output_dir):
    """output_dir pre-seeded with the baseline optional CSVs required to
    get generate_mnc_report to completion (see write_baseline_optional_csvs).
    """
    write_baseline_optional_csvs(output_dir)
    return output_dir


def render_context(mock_docx_template, mock_doc_instance):
    return mock_doc_instance.render.call_args[0][0]


@patch("ecoscope_workflows_ext_mnc.tasks.reporting._overall_report.DocxTemplate")
class TestGenerateMncReportValidation:
    """Path validation happens before DocxTemplate is touched."""

    def test_empty_template_path_raises(self, mock_docx_template, output_dir):
        with pytest.raises(ValueError, match="template_path"):
            generate_mnc_report(template_path="   ", output_dir=str(output_dir))

    def test_empty_output_dir_raises(self, mock_docx_template, template_path):
        with pytest.raises(ValueError, match="output_directory"):
            generate_mnc_report(template_path=template_path, output_dir="   ")

    def test_missing_template_file_raises(self, mock_docx_template, tmp_path, output_dir):
        missing = tmp_path / "does_not_exist.docx"
        with pytest.raises(FileNotFoundError):
            generate_mnc_report(template_path=str(missing), output_dir=str(output_dir))

    def test_output_dir_is_created_if_missing(self, mock_docx_template, template_path, tmp_path):
        new_dir = tmp_path / "brand_new"
        assert not new_dir.exists()

        # The directory is created up front, before any CSV scanning; the
        # AttributeError below comes later, once report-building reaches the
        # unguarded optional CSVs documented in TestGenerateMncReportKnownFragility.
        with pytest.raises(AttributeError):
            generate_mnc_report(template_path=template_path, output_dir=str(new_dir))

        assert new_dir.exists()


@patch("ecoscope_workflows_ext_mnc.tasks.reporting._overall_report.InlineImage")
@patch("ecoscope_workflows_ext_mnc.tasks.reporting._overall_report.DocxTemplate")
class TestGenerateMncReportContext:
    """Verifies the render context built from files discovered in output_dir.

    All tests here use full_output_dir (pre-seeded with the baseline
    optional CSVs) rather than the bare output_dir fixture, since without
    them generate_mnc_report raises before completing — see
    TestGenerateMncReportKnownFragility.
    """

    def test_render_and_save_called_with_defaults_when_dir_is_empty(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        result_path = generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        mock_docx_template.assert_called_once_with(template_path)
        mock_doc_instance.render.assert_called_once()
        mock_doc_instance.save.assert_called_once()
        assert result_path == mock_doc_instance.save.call_args[0][0]
        assert str(result_path).startswith(str(full_output_dir))
        assert str(result_path).endswith(".docx")

    def test_default_filename_pattern(self, mock_docx_template, mock_inline_image, template_path, full_output_dir):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        result_path = generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        assert Path(result_path).name.startswith("overall_report_")

    def test_custom_filename_used_verbatim(self, mock_docx_template, mock_inline_image, template_path, full_output_dir):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        result_path = generate_mnc_report(
            template_path=template_path, output_dir=str(full_output_dir), filename="custom_name.docx"
        )

        assert Path(result_path) == Path(full_output_dir) / "custom_name.docx"

    def test_defaults_are_zero_when_no_other_csvs_present(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["total_events"] == 0
        assert ctx["no_of_foot_patrols"] == 0
        assert ctx["total_foot_patrol_hours"] == 0.0
        assert ctx["no_of_vehicle_patrols"] == 0
        assert ctx["no_of_motor_patrols"] == 0
        assert ctx["mara_conservancy_percentage"] == 0.0
        assert ctx["night_patrols_percent"] == 0.0
        assert ctx["total_livestock_predation_events"] == 0
        assert ctx["total_wildlife_incidents"] == 0
        assert ctx["patrol_efforts"] == []

    def test_table_csv_loaded_as_list_of_records(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "patrol_purpose_summary",
            pd.DataFrame({"purpose": ["night", "routine"], "no_of_patrols": [3, 7]}),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["patrol_purpose"] == [
            {"purpose": "night", "no_of_patrols": 3},
            {"purpose": "routine", "no_of_patrols": 7},
        ]

    def test_table_csv_nulls_filled_with_zero(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "overall_patrol_efforts",
            pd.DataFrame({"no_of_patrols": [5, None]}),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["patrol_efforts"][1]["no_of_patrols"] == 0

    def test_total_events_reads_last_row(self, mock_docx_template, mock_inline_image, template_path, full_output_dir):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "total_events_recorded_by_date",
            pd.DataFrame({"no_of_events": [10, 25, 42]}),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["total_events"] == 42

    def test_foot_patrol_efforts_are_summed(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "foot_patrol_efforts",
            pd.DataFrame(
                {
                    "no_of_patrols": [2, 3],
                    "duration_hrs": [1.5, 2.5],
                    "distance_km": [4.0, 6.0],
                    "average_speed": [1.0, 2.0],
                }
            ),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["no_of_foot_patrols"] == 5
        assert ctx["total_foot_patrol_hours"] == 4.0
        assert ctx["total_foot_patrol_distance"] == 10.0
        assert ctx["average_foot_patrol_speed"] == 3.0

    def test_mara_conservancy_percentage_extracted(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "patrol_coverage",
            pd.DataFrame(
                {
                    "conservancy_name": ["Other Conservancy", "Mara North Conservancy"],
                    "occupancy_percentage": [10.0, 87.65],
                }
            ),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["mara_conservancy_percentage"] == 87.65

    def test_mara_conservancy_percentage_defaults_when_row_missing(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "patrol_coverage",
            pd.DataFrame({"conservancy_name": ["Other Conservancy"], "occupancy_percentage": [10.0]}),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["mara_conservancy_percentage"] == 0.0

    def test_patrol_purpose_percentages_computed_against_last_row_total(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        # The implementation treats the *last* row's no_of_patrols as the
        # denominator, regardless of its purpose label.
        write_csv(
            full_output_dir,
            "patrol_purpose_summary",
            pd.DataFrame(
                {
                    "purpose": ["night", "routine", "joint", "total"],
                    "no_of_patrols": [10, 20, 20, 50],
                }
            ),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["night_patrols_percent"] == pytest.approx(20.0)
        assert ctx["routine_patrols_percent"] == pytest.approx(40.0)
        assert ctx["joint_patrols_percent"] == pytest.approx(40.0)

    def test_wildlife_incidents_summed_from_records_column(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        write_csv(
            full_output_dir,
            "wildlife_incidents_summary_table",
            pd.DataFrame({"event_type": ["Fire", "Snare"], "records": [3, None]}),
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["total_wildlife_incidents"] == 3

    def test_image_found_creates_inline_image(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance
        (Path(full_output_dir) / "temperature_readings_over_time.png").touch()

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["temperature_chart"] is not None
        mock_inline_image.assert_called()

    def test_image_missing_context_value_is_none(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["temperature_chart"] is None

    def test_missing_image_warning_printed_when_validate_images_true(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir, capsys
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir), validate_images=True)

        assert "Image not found for temperature_chart" in capsys.readouterr().out

    def test_missing_image_warning_suppressed_when_validate_images_false(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir, capsys
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir), validate_images=False)

        assert "Image not found for temperature_chart" not in capsys.readouterr().out

    def test_generated_by_sets_er_user(self, mock_docx_template, mock_inline_image, template_path, full_output_dir):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir), generated_by="jdoe")

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["er_user"] == "jdoe"

    def test_generated_by_omitted_when_not_provided(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert "er_user" not in ctx

    def test_time_period_formats_time_range_and_short_range(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        time_period = FakeTimeRange(
            since=datetime(2024, 1, 1, tzinfo=timezone.utc),
            until=datetime(2024, 1, 31, tzinfo=timezone.utc),
            time_format="%Y-%m-%d",
        )

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir), time_period=time_period)

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["time_range"] == "2024-01-01 to 2024-01-31"
        assert ctx["time_period"] == "2024-01-01 - 2024-01-31"

    def test_time_period_none_leaves_context_values_none(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir), time_period=None)

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["time_range"] is None
        assert ctx["time_period"] is None

    def test_ignores_files_in_nested_subdirectories(
        self, mock_docx_template, mock_inline_image, template_path, full_output_dir
    ):
        # os.walk descends into subdirectories, so a CSV placed in a nested
        # folder is still discovered by file stem.
        nested = Path(full_output_dir) / "nested"
        nested.mkdir()
        pd.DataFrame({"no_of_events": [7]}).to_csv(nested / "total_events_recorded_by_date.csv", index=False)

        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        generate_mnc_report(template_path=template_path, output_dir=str(full_output_dir))

        ctx = render_context(mock_docx_template, mock_doc_instance)
        assert ctx["total_events"] == 7


@patch("ecoscope_workflows_ext_mnc.tasks.reporting._overall_report.InlineImage")
@patch("ecoscope_workflows_ext_mnc.tasks.reporting._overall_report.DocxTemplate")
class TestGenerateMncReportKnownFragility:
    """Documents current crash behavior for optional CSVs that are read
    without a None-check before use. These are not intentional contracts;
    they pin down today's (fragile) behavior so a future fix is visible
    as an intentional test change rather than a silent regression.
    """

    def test_missing_lion_summary_csv_currently_raises(
        self, mock_docx_template, mock_inline_image, template_path, output_dir
    ):
        mock_doc_instance = MagicMock()
        mock_docx_template.return_value = mock_doc_instance

        with pytest.raises(AttributeError):
            generate_mnc_report(template_path=template_path, output_dir=str(output_dir))
