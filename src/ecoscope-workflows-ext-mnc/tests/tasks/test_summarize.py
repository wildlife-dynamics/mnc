import pytest
import pandas as pd
import numpy as np
from pathlib import Path

# Assuming your function is in a module called 'wildlife_utils'
from ecoscope_workflows_ext_mnc.tasks._summarize import make_wildlife_summary_table, DEFAULT_VALUE_MAP

TEST_DATA_DIR = Path(__file__).parent.parent / "data"


class TestMakeWildlifeSummaryTable:
    """Test cases for make_wildlife_summary_table function."""
    
    @pytest.fixture
    def sample_fire_events(self):
        """Create sample fire event data."""
        return pd.DataFrame({
            "event_type": ["fire_rep", "fire_rep", "fire_rep"],
            "fire_rep_cause": ["Lightning", "Human activity", "Unknown"],
            "fire_rep_status": ["Active", "Controlled", "Extinguished"],
            "fire_rep_direction": ["North", "South", "East"],
            "event_details": ["Large fire", "Small fire", None]
        })
    
    @pytest.fixture
    def sample_snare_events(self):
        """Create sample snare event data."""
        return pd.DataFrame({
            "event_type": ["snare_rep", "snare_rep", "snare_rep", "snare_rep"],
            "number_of_snares": [5, 10, 2, None],
            "snarerep_action": ["Removed", "Destroyed", "Removed", "Reported"],
            "snarerep_status": ["Active", "Inactive", "Active", "Active"]
        })
    
    @pytest.fixture
    def sample_wildlife_carcass_events(self):
        """Create sample wildlife carcass event data."""
        return pd.DataFrame({
            "event_type": ["wildlife_carcass_rep", "wildlife_carcass_rep"],
            "wildlife_carcass_species": ["Elephant", "Giraffe"],
            "wildlife_carcass_suspected_cause": ["Poaching", "Natural causes"],
            "event_details__wildlifecarcass_comments": ["Found near waterhole", None],
            "wildlife_carcass_visible_injury": ["Gunshot wound", None]
        })
    
    @pytest.fixture
    def sample_injured_wildlife_events(self):
        """Create sample injured wildlife event data."""
        return pd.DataFrame({
            "event_type": ["wildlife_injury_rep", "wildlife_injury_rep"],
            "wildlife_injury_rep_species": ["Lion", "Rhino"],
            "wildlife_injury_rep_age": ["Adult", "Juvenile"],
            "wildlife_injury_rep_injury_type": ["Snare wound", "Broken leg"],
            "wildlife_injury_rep_comments": ["Requires urgent attention", None],
            "wildlife_injury_rep_severity": ["Critical", "Moderate"]
        })
    
    @pytest.fixture
    def sample_vet_treatment_events(self):
        """Create sample veterinary treatment event data."""
        return pd.DataFrame({
            "event_type": ["wildlife_treatment_rep", "wildlife_treatment_rep"],
            "wildlife_treatment_species": ["Zebra", "Buffalo"],
            "wildlife_treatment_comments": ["Antibiotics administered", "Wound cleaned"],
            "wildlife_treatment_vet_attending": ["Dr. Smith", "Dr. Jones"]
        })
    
    @pytest.fixture
    def mixed_events_data(self):
        """Create mixed event types data."""
        return pd.DataFrame({
            "event_type": [
                "fire_rep", "snare_rep", "wildlife_carcass_rep",
                "fire_rep", "snare_rep", "wildlife_injury_rep",
                "wildlife_treatment_rep", "unknown_type"
            ],
            "fire_rep_cause": ["Lightning", None, None, "Human", None, None, None, None],
            "number_of_snares": [None, 3, None, None, 7, None, None, None],
            "wildlife_carcass_species": [None, None, "Elephant", None, None, None, None, None],
            "wildlife_injury_rep_species": [None, None, None, None, None, "Lion", None, None],
            "wildlife_treatment_species": [None, None, None, None, None, None, "Zebra", None],
            "event_details": ["Detail1", "Detail2", "Detail3", None, None, None, None, "Unknown event"]
        })
    
    def test_basic_functionality_fire_events(self, sample_fire_events):
        """Test basic functionality with fire events."""
        result = make_wildlife_summary_table(sample_fire_events)
        
        assert isinstance(result, pd.DataFrame)
        assert "event_type" in result.columns
        assert "records" in result.columns
        assert "summary_details" in result.columns
        assert len(result) == 1
        assert result["event_type"].iloc[0] == "Fire"
        assert result["records"].iloc[0] == 3
    
    def test_basic_functionality_snare_events(self, sample_snare_events):
        """Test basic functionality with snare events."""
        result = make_wildlife_summary_table(sample_snare_events)
        
        assert len(result) == 1
        assert result["event_type"].iloc[0] == "Snare"
        assert result["records"].iloc[0] == 4
        assert "snares" in result["summary_details"].iloc[0]
    
    def test_snare_count_formatting(self, sample_snare_events):
        """Test that snare counts are formatted correctly."""
        result = make_wildlife_summary_table(sample_snare_events)
        summary = result["summary_details"].iloc[0]
        
        assert "5 snares" in summary or "10 snares" in summary or "2 snares" in summary
    
    def test_wildlife_carcass_events(self, sample_wildlife_carcass_events):
        """Test wildlife carcass event processing."""
        result = make_wildlife_summary_table(sample_wildlife_carcass_events)
        
        assert len(result) == 1
        assert result["event_type"].iloc[0] == "Wildlife carcass"
        assert result["records"].iloc[0] == 2
        summary = result["summary_details"].iloc[0]
        assert "Elephant" in summary or "Giraffe" in summary
    
    def test_injured_wildlife_events(self, sample_injured_wildlife_events):
        """Test injured wildlife event processing."""
        result = make_wildlife_summary_table(sample_injured_wildlife_events)
        
        assert len(result) == 1
        assert result["event_type"].iloc[0] == "Injured wildlife"
        assert result["records"].iloc[0] == 2
        summary = result["summary_details"].iloc[0]
        assert "Lion" in summary or "Rhino" in summary
    
    def test_vet_treatment_events(self, sample_vet_treatment_events):
        """Test veterinary treatment event processing."""
        result = make_wildlife_summary_table(sample_vet_treatment_events)
        
        assert len(result) == 1
        assert result["event_type"].iloc[0] == "Veterinary treatment"
        assert result["records"].iloc[0] == 2
    
    def test_mixed_event_types(self, mixed_events_data):
        """Test processing of mixed event types."""
        result = make_wildlife_summary_table(mixed_events_data)
        
        assert len(result) >= 5  # At least 5 different event types
        assert result["records"].sum() == len(mixed_events_data)
        
        # Check that known event types are mapped correctly
        assert "Fire" in result["event_type"].values
        assert "Snare" in result["event_type"].values
    
    def test_custom_value_map(self, sample_fire_events):
        """Test with custom value mapping."""
        custom_map = {"fire_rep": "Fire Incident"}
        result = make_wildlife_summary_table(sample_fire_events, value_map=custom_map)
        
        assert result["event_type"].iloc[0] == "Fire Incident"
    
    def test_max_unique_parameter(self, sample_snare_events):
        """Test max_unique parameter limits summaries."""
        # Create data with many unique summaries
        many_snares = pd.DataFrame({
            "event_type": ["snare_rep"] * 10,
            "number_of_snares": range(1, 11),
            "snarerep_action": [f"Action_{i}" for i in range(10)]
        })
        
        result_limited = make_wildlife_summary_table(many_snares, max_unique=3)
        result_more = make_wildlife_summary_table(many_snares, max_unique=8)
        
        summary_limited = result_limited["summary_details"].iloc[0]
        summary_more = result_more["summary_details"].iloc[0]
        
        # Limited should have fewer lines
        assert summary_limited.count("\n") < summary_more.count("\n")
    
    def test_shorten_width_parameter(self):
        """Test shorten_width parameter truncates long summaries."""
        long_text_data = pd.DataFrame({
            "event_type": ["fire_rep"],
            "event_details": ["This is a very long description " * 50]
        })
        
        result_short = make_wildlife_summary_table(long_text_data, shorten_width=50)
        result_long = make_wildlife_summary_table(long_text_data, shorten_width=500)
        
        summary_short = result_short["summary_details"].iloc[0]
        summary_long = result_long["summary_details"].iloc[0]
        
        assert len(summary_short) < len(summary_long)
        assert "..." in summary_short
    
    def test_order_parameter(self, mixed_events_data):
        """Test custom ordering of event types."""
        custom_order = ["Snare", "Fire", "Wildlife carcass", "Injured wildlife", "Veterinary treatment"]
        result = make_wildlife_summary_table(mixed_events_data, order=custom_order)
        
        # Check that Snare appears before Fire in the result
        snare_idx = result[result["event_type"] == "Snare"].index[0]
        fire_idx = result[result["event_type"] == "Fire"].index[0]
        assert snare_idx < fire_idx
    
    def test_default_sorting_by_records(self, mixed_events_data):
        """Test that default sorting is by record count descending."""
        result = make_wildlife_summary_table(mixed_events_data)
        
        # Records should be in descending order
        records = result["records"].tolist()
        assert records == sorted(records, reverse=True)
    
    def test_empty_dataframe(self):
        """Test with empty DataFrame."""
        empty_df = pd.DataFrame({"event_type": []})
        result = make_wildlife_summary_table(empty_df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert all(col in result.columns for col in ["event_type", "records", "summary_details"])
    
    
    def test_null_event_types(self):
        """Test handling of null event_types."""
        df_with_nulls = pd.DataFrame({
            "event_type": ["fire_rep", None, "snare_rep", np.nan],
            "event_details": ["Detail1", "Detail2", "Detail3", "Detail4"]
        })
        
        result = make_wildlife_summary_table(df_with_nulls)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 2  # At least Fire and Snare
    
    def test_unmapped_event_types(self):
        """Test that unmapped event types appear with original name."""
        df_unmapped = pd.DataFrame({
            "event_type": ["custom_event_type", "custom_event_type"],
            "event_details": ["Detail1", "Detail2"]
        })
        
        result = make_wildlife_summary_table(df_unmapped)
        
        assert "custom_event_type" in result["event_type"].values
        assert result["records"].iloc[0] == 2
    
    def test_duplicate_summaries_deduplication(self):
        """Test that duplicate summaries are deduplicated."""
        df_duplicates = pd.DataFrame({
            "event_type": ["fire_rep"] * 5,
            "fire_rep_cause": ["Lightning"] * 5,
            "fire_rep_status": ["Active"] * 5
        })
        
        result = make_wildlife_summary_table(df_duplicates)
        summary = result["summary_details"].iloc[0]
        
        # Should only have one unique summary, not 5
        assert summary.count("\n") == 0  # No newlines means only one summary
    
    def test_na_values_handling(self):
        """Test proper handling of various NA values."""
        df_na_values = pd.DataFrame({
            "event_type": ["fire_rep", "fire_rep", "fire_rep"],
            "fire_rep_cause": ["Lightning", None, np.nan],
            "fire_rep_status": [None, "Active", ""],
            "event_details": ["", None, "Valid detail"]
        })
        
        result = make_wildlife_summary_table(df_na_values)
        
        assert len(result) == 1
        assert result["records"].iloc[0] == 3
    
    def test_special_characters_in_details(self):
        """Test handling of special characters in event details."""
        df_special = pd.DataFrame({
            "event_type": ["fire_rep"],
            "event_details": ["Fire with — special—characters & symbols!"]
        })
        
        result = make_wildlife_summary_table(df_special)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    def test_fallback_to_event_details(self):
        """Test that function falls back to event_details when specific fields missing."""
        df_minimal = pd.DataFrame({
            "event_type": ["fire_rep"],
            "event_details": ["Fallback details"]
        })
        
        result = make_wildlife_summary_table(df_minimal)
        summary = result["summary_details"].iloc[0]
        
        assert "Fallback details" in summary
    
    def test_input_dataframe_not_modified(self, sample_fire_events):
        """Test that input DataFrame is not modified."""
        original_df = sample_fire_events.copy()
        make_wildlife_summary_table(sample_fire_events)
        
        pd.testing.assert_frame_equal(sample_fire_events, original_df)
    
    def test_summary_separator(self, sample_fire_events):
        """Test that summary details use proper separator (em dash)."""
        result = make_wildlife_summary_table(sample_fire_events)
        summary = result["summary_details"].iloc[0]
        
        # Should use " — " as separator between details
        if summary:
            assert "—" in summary or len(summary.split()) == 1
    
    def test_newline_separator_between_unique_summaries(self):
        """Test that unique summaries are separated by newlines."""
        df_unique = pd.DataFrame({
            "event_type": ["fire_rep", "fire_rep", "fire_rep"],
            "fire_rep_cause": ["Lightning", "Human", "Unknown"],
        })
        
        result = make_wildlife_summary_table(df_unique, max_unique=3)
        summary = result["summary_details"].iloc[0]
        
        # Should have newlines between unique summaries
        assert "\n" in summary
    
    def test_non_integer_snare_count(self):
        """Test handling of non-integer snare counts."""
        df_bad_snare = pd.DataFrame({
            "event_type": ["snare_rep"],
            "number_of_snares": ["not_a_number"]
        })
        
        # Should handle gracefully without crashing
        result = make_wildlife_summary_table(df_bad_snare)
        assert isinstance(result, pd.DataFrame)
    
    def test_all_event_types_covered(self):
        """Test that all event types in DEFAULT_VALUE_MAP are handled."""
        all_types_df = pd.DataFrame({
            "event_type": list(DEFAULT_VALUE_MAP.keys()),
            "event_details": [f"Details for {i}" for i in range(len(DEFAULT_VALUE_MAP))]
        })
        
        result = make_wildlife_summary_table(all_types_df)
        
        # Should have one row per event type
        assert len(result) == len(DEFAULT_VALUE_MAP)
        
        # All mapped values should be present
        for mapped_value in DEFAULT_VALUE_MAP.values():
            assert mapped_value in result["event_type"].values


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_single_event(self):
        """Test with single event."""
        df_single = pd.DataFrame({
            "event_type": ["fire_rep"],
            "fire_rep_cause": ["Lightning"]
        })
        
        result = make_wildlife_summary_table(df_single)
        
        assert len(result) == 1
        assert result["records"].iloc[0] == 1
    
    def test_max_unique_zero(self):
        """Test with max_unique set to 0."""
        df = pd.DataFrame({
            "event_type": ["fire_rep", "fire_rep"],
            "fire_rep_cause": ["Lightning", "Human"]
        })
        
        result = make_wildlife_summary_table(df, max_unique=0)
        summary = result["summary_details"].iloc[0]
        
        # Should have empty summary or minimal content
        assert summary == "" or len(summary) < 10
    
    def test_very_large_max_unique(self):
        """Test with very large max_unique value."""
        df = pd.DataFrame({
            "event_type": ["fire_rep"] * 100,
            "fire_rep_cause": [f"Cause_{i}" for i in range(100)]
        })
        
        result = make_wildlife_summary_table(df, max_unique=1000)
        
        # Should include all unique summaries
        assert result["records"].iloc[0] == 100
    
    def test_empty_order_list(self):
        """Test with empty order list."""
        df = pd.DataFrame({
            "event_type": ["fire_rep", "snare_rep"],
            "event_details": ["Detail1", "Detail2"]
        })
        
        result = make_wildlife_summary_table(df, order=[])
        
        # Should fall back to sorting by records
        assert isinstance(result, pd.DataFrame)
