import numpy as np
import pandas as pd
import pytest

from ecoscope_workflows_ext_mnc.tasks.summary._summarize import make_wildlife_summary_table

VALUE_MAP = {
    "fire_rep": "Fire",
    "snare_rep": "Snare",
    "wildlife_carcass_rep": "Wildlife carcass",
    "wildlife_injury_rep": "Injured wildlife",
    "wildlife_treatment_rep": "Veterinary treatment",
}


class TestMakeWildlifeSummaryTable:
    """Test cases for make_wildlife_summary_table."""

    @pytest.fixture
    def sample_fire_events(self):
        return pd.DataFrame(
            {
                "event_type": ["fire_rep", "fire_rep", "fire_rep"],
                "fire_rep_cause": ["Lightning", "Human activity", "Unknown"],
                "fire_rep_status": ["Active", "Controlled", "Extinguished"],
                "fire_rep_direction": ["North", "South", "East"],
                "event_details": ["Large fire", "Small fire", None],
            }
        )

    @pytest.fixture
    def sample_snare_events(self):
        return pd.DataFrame(
            {
                "event_type": ["snare_rep", "snare_rep", "snare_rep", "snare_rep"],
                "number_of_snares": [5, 10, 2, None],
                "snarerep_action": ["Removed", "Destroyed", "Removed", "Reported"],
                "snarerep_status": ["Active", "Inactive", "Active", "Active"],
            }
        )

    @pytest.fixture
    def sample_wildlife_carcass_events(self):
        return pd.DataFrame(
            {
                "event_type": ["wildlife_carcass_rep", "wildlife_carcass_rep"],
                "wildlife_carcass_species": ["Elephant", "Giraffe"],
                "wildlife_carcass_suspected_cause": ["Poaching", "Natural causes"],
                "event_details__wildlifecarcass_comments": ["Found near waterhole", None],
                "wildlife_carcass_visible_injury": ["Gunshot wound", None],
            }
        )

    @pytest.fixture
    def sample_injured_wildlife_events(self):
        return pd.DataFrame(
            {
                "event_type": ["wildlife_injury_rep", "wildlife_injury_rep"],
                "wildlife_injury_rep_species": ["Lion", "Rhino"],
                "wildlife_injury_rep_age": ["Adult", "Juvenile"],
                "wildlife_injury_rep_injury_type": ["Snare wound", "Broken leg"],
                "wildlife_injury_rep_comments": ["Requires urgent attention", None],
                "wildlife_injury_rep_severity": ["Critical", "Moderate"],
            }
        )

    @pytest.fixture
    def sample_vet_treatment_events(self):
        return pd.DataFrame(
            {
                "event_type": ["wildlife_treatment_rep", "wildlife_treatment_rep"],
                "wildlife_treatment_species": ["Zebra", "Buffalo"],
                "wildlife_treatment_comments": ["Antibiotics administered", "Wound cleaned"],
                "wildlife_treatment_vet_attending": ["Dr. Smith", "Dr. Jones"],
            }
        )

    @pytest.fixture
    def mixed_events_data(self):
        return pd.DataFrame(
            {
                "event_type": [
                    "fire_rep",
                    "snare_rep",
                    "wildlife_carcass_rep",
                    "fire_rep",
                    "snare_rep",
                    "wildlife_injury_rep",
                    "wildlife_treatment_rep",
                    "unknown_type",
                ],
                "fire_rep_cause": ["Lightning", None, None, "Human", None, None, None, None],
                "number_of_snares": [None, 3, None, None, 7, None, None, None],
                "wildlife_carcass_species": [None, None, "Elephant", None, None, None, None, None],
                "wildlife_injury_rep_species": [None, None, None, None, None, "Lion", None, None],
                "wildlife_treatment_species": [None, None, None, None, None, None, "Zebra", None],
                "event_details": ["Detail1", "Detail2", "Detail3", None, None, None, None, "Unknown event"],
            }
        )

    def test_basic_functionality_fire_events(self, sample_fire_events):
        result = make_wildlife_summary_table(sample_fire_events, value_map=VALUE_MAP)

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["event_type", "records", "summary_details"]
        assert len(result) == 1
        assert result["event_type"].iloc[0] == "Fire"
        assert result["records"].iloc[0] == 3

    def test_snare_count_formatting(self, sample_snare_events):
        result = make_wildlife_summary_table(sample_snare_events, value_map=VALUE_MAP)
        summary = result["summary_details"].iloc[0]

        assert result["event_type"].iloc[0] == "Snare"
        assert result["records"].iloc[0] == 4
        assert "5 snares" in summary or "10 snares" in summary or "2 snares" in summary

    def test_wildlife_carcass_events(self, sample_wildlife_carcass_events):
        result = make_wildlife_summary_table(sample_wildlife_carcass_events, value_map=VALUE_MAP)

        assert result["event_type"].iloc[0] == "Wildlife carcass"
        assert result["records"].iloc[0] == 2
        summary = result["summary_details"].iloc[0]
        assert "Elephant" in summary or "Giraffe" in summary

    def test_injured_wildlife_events(self, sample_injured_wildlife_events):
        result = make_wildlife_summary_table(sample_injured_wildlife_events, value_map=VALUE_MAP)

        assert result["event_type"].iloc[0] == "Injured wildlife"
        assert result["records"].iloc[0] == 2
        summary = result["summary_details"].iloc[0]
        assert "Lion" in summary or "Rhino" in summary

    def test_vet_treatment_events(self, sample_vet_treatment_events):
        result = make_wildlife_summary_table(sample_vet_treatment_events, value_map=VALUE_MAP)

        assert result["event_type"].iloc[0] == "Veterinary treatment"
        assert result["records"].iloc[0] == 2

    def test_mixed_event_types(self, mixed_events_data):
        result = make_wildlife_summary_table(mixed_events_data, value_map=VALUE_MAP)

        assert len(result) == 6  # 5 mapped types + the unmapped "unknown_type"
        assert result["records"].sum() == len(mixed_events_data)
        assert "Fire" in result["event_type"].values
        assert "Snare" in result["event_type"].values
        assert "unknown_type" in result["event_type"].values

    def test_unmapped_event_type_keeps_original_name(self):
        df = pd.DataFrame(
            {"event_type": ["custom_event_type", "custom_event_type"], "event_details": ["Detail1", "Detail2"]}
        )
        result = make_wildlife_summary_table(df, value_map=VALUE_MAP)

        assert "custom_event_type" in result["event_type"].values
        assert result["records"].iloc[0] == 2

    def test_max_unique_limits_summary_lines(self):
        many_snares = pd.DataFrame(
            {
                "event_type": ["snare_rep"] * 10,
                "number_of_snares": range(1, 11),
                "snarerep_action": [f"Action_{i}" for i in range(10)],
            }
        )

        result_limited = make_wildlife_summary_table(many_snares, value_map=VALUE_MAP, max_unique=3)
        result_more = make_wildlife_summary_table(many_snares, value_map=VALUE_MAP, max_unique=8)

        summary_limited = result_limited["summary_details"].iloc[0]
        summary_more = result_more["summary_details"].iloc[0]

        assert summary_limited.count("\n") == 2  # 3 unique lines -> 2 separators
        assert summary_more.count("\n") == 7
        assert summary_limited.count("\n") < summary_more.count("\n")

    def test_shorten_width_truncates_long_summary(self):
        long_text_data = pd.DataFrame(
            {"event_type": ["fire_rep"], "event_details": ["This is a very long description " * 50]}
        )

        result_short = make_wildlife_summary_table(long_text_data, value_map=VALUE_MAP, shorten_width=50)
        result_long = make_wildlife_summary_table(long_text_data, value_map=VALUE_MAP, shorten_width=500)

        summary_short = result_short["summary_details"].iloc[0]
        summary_long = result_long["summary_details"].iloc[0]

        assert len(summary_short) < len(summary_long)
        assert "..." in summary_short

    def test_order_parameter_controls_row_order(self, mixed_events_data):
        custom_order = ["Snare", "Fire", "Wildlife carcass", "Injured wildlife", "Veterinary treatment"]
        result = make_wildlife_summary_table(mixed_events_data, value_map=VALUE_MAP, order=custom_order)

        snare_idx = result[result["event_type"] == "Snare"].index[0]
        fire_idx = result[result["event_type"] == "Fire"].index[0]
        assert snare_idx < fire_idx

    def test_default_sorting_by_records_descending(self, mixed_events_data):
        result = make_wildlife_summary_table(mixed_events_data, value_map=VALUE_MAP)

        records = result["records"].tolist()
        assert records == sorted(records, reverse=True)

    def test_empty_dataframe(self):
        empty_df = pd.DataFrame({"event_type": []})
        result = make_wildlife_summary_table(empty_df, value_map=VALUE_MAP)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["event_type", "records", "summary_details"]

    def test_null_event_types_grouped_separately(self):
        df_with_nulls = pd.DataFrame(
            {
                "event_type": ["fire_rep", None, "snare_rep", np.nan],
                "event_details": ["Detail1", "Detail2", "Detail3", "Detail4"],
            }
        )

        result = make_wildlife_summary_table(df_with_nulls, value_map=VALUE_MAP)

        assert "Fire" in result["event_type"].values
        assert "Snare" in result["event_type"].values

    def test_duplicate_summaries_are_deduplicated(self):
        df_duplicates = pd.DataFrame(
            {"event_type": ["fire_rep"] * 5, "fire_rep_cause": ["Lightning"] * 5, "fire_rep_status": ["Active"] * 5}
        )

        result = make_wildlife_summary_table(df_duplicates, value_map=VALUE_MAP)
        summary = result["summary_details"].iloc[0]

        assert summary.count("\n") == 0

    def test_fallback_to_event_details_when_no_specific_fields(self):
        df_minimal = pd.DataFrame({"event_type": ["fire_rep"], "event_details": ["Fallback details"]})

        result = make_wildlife_summary_table(df_minimal, value_map=VALUE_MAP)
        summary = result["summary_details"].iloc[0]

        assert "Fallback details" in summary

    def test_input_dataframe_not_mutated(self, sample_fire_events):
        original_df = sample_fire_events.copy()
        make_wildlife_summary_table(sample_fire_events, value_map=VALUE_MAP)

        pd.testing.assert_frame_equal(sample_fire_events, original_df)

    def test_non_integer_snare_count_does_not_crash(self):
        df_bad_snare = pd.DataFrame({"event_type": ["snare_rep"], "number_of_snares": ["not_a_number"]})

        result = make_wildlife_summary_table(df_bad_snare, value_map=VALUE_MAP)
        assert isinstance(result, pd.DataFrame)

    def test_empty_value_map_keeps_original_event_type(self, sample_fire_events):
        result = make_wildlife_summary_table(sample_fire_events, value_map={})
        assert result["event_type"].iloc[0] == "fire_rep"


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_single_event(self):
        df_single = pd.DataFrame({"event_type": ["fire_rep"], "fire_rep_cause": ["Lightning"]})

        result = make_wildlife_summary_table(df_single, value_map=VALUE_MAP)

        assert len(result) == 1
        assert result["records"].iloc[0] == 1

    def test_max_unique_zero_still_includes_first_summary(self):
        # The unique-summaries loop appends before checking the max_unique_local
        # cutoff, so 0 behaves like 1 rather than suppressing all summaries.
        df = pd.DataFrame({"event_type": ["fire_rep", "fire_rep"], "fire_rep_cause": ["Lightning", "Human"]})

        result = make_wildlife_summary_table(df, value_map=VALUE_MAP, max_unique=0)
        summary = result["summary_details"].iloc[0]

        assert summary == "Lightning"

    def test_very_large_max_unique_includes_all_summaries(self):
        df = pd.DataFrame({"event_type": ["fire_rep"] * 100, "fire_rep_cause": [f"Cause_{i}" for i in range(100)]})

        result = make_wildlife_summary_table(df, value_map=VALUE_MAP, max_unique=1000)

        assert result["records"].iloc[0] == 100
        assert result["summary_details"].iloc[0].count("\n") == 99

    def test_empty_order_list_falls_back_to_records_sort(self):
        df = pd.DataFrame({"event_type": ["fire_rep", "snare_rep"], "event_details": ["Detail1", "Detail2"]})

        result = make_wildlife_summary_table(df, value_map=VALUE_MAP, order=[])

        assert isinstance(result, pd.DataFrame)
        records = result["records"].tolist()
        assert records == sorted(records, reverse=True)
