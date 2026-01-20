import pandas as pd
from textwrap import shorten
from ecoscope_workflows_core.decorators import task
from typing import Dict, Iterable, List, Optional
from ecoscope_workflows_core.annotations import AnyDataFrame


DEFAULT_VALUE_MAP: Dict[str, str] = {
    "fire_rep": "Fire",
    "snare_rep": "Snare",
    "wildlife_carcass_rep": "Wildlife carcass",
    "wildlife_injury_rep": "Injured wildlife",
    "wildlife_treatment_rep": "Veterinary treatment",
}


@task
def make_wildlife_summary_table(
    df: AnyDataFrame,
    value_map: Dict[str, str] = DEFAULT_VALUE_MAP,
    max_unique: int = 6,
    shorten_width: int = 300,
    order: Optional[Iterable[str]] = None,
) -> AnyDataFrame:
    """
    Build and return a grouped summary DataFrame for event types.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing event rows. Must contain at least "event_type".
    value_map : dict, optional
        Mapping from raw event_type keys to human labels (default: DEFAULT_VALUE_MAP).
    max_unique : int, optional
        Max number of unique row summaries to include per group (default: 6).
    shorten_width : int, optional
        Maximum length of each row summary (passed to textwrap.shorten).
    order : iterable of str, optional
        Desired order for event_type categories (if provided, other types appear after).

    Returns
    -------
    pd.DataFrame
        Grouped dataframe with columns: event_type, records, summary_details
    """
    # defensive copy
    df = df.copy()

    def summarize_row(row: pd.Series) -> str:
        et = row.get("event_type_mapped")
        pieces: List[str] = []

        if et == "Fire":
            for c in ("fire_rep_cause", "fire_rep_status", "fire_rep_direction", "event_details"):
                val = row.get(c) or row.get("event_details__wildlifecarcass_comments")
                if val and pd.notna(val):
                    pieces.append(str(val))

        elif et == "Snare":
            if pd.notna(row.get("number_of_snares")):
                try:
                    pieces.append(f"{int(row['number_of_snares'])} snares")
                except Exception:
                    pieces.append(str(row.get("number_of_snares")))
            for c in ("snarerep_action", "snarerep_status"):
                val = row.get(c)
                if val and pd.notna(val):
                    pieces.append(str(val))

        elif et == "Wildlife carcass":
            for c in (
                "wildlife_carcass_species",
                "wildlife_carcass_suspected_cause",
                "event_details__wildlifecarcass_comments",
                "wildlife_carcass_visible_injury",
            ):
                val = row.get(c)
                if val and pd.notna(val):
                    pieces.append(str(val))

        elif et == "Injured wildlife":
            for c in (
                "wildlife_injury_rep_species",
                "wildlife_injury_rep_age",
                "wildlife_injury_rep_injury_type",
                "wildlife_injury_rep_comments",
                "wildlife_injury_rep_severity",
            ):
                val = row.get(c)
                if val and pd.notna(val):
                    pieces.append(str(val))

        elif et == "Veterinary treatment":
            for c in ("wildlife_treatment_species", "wildlife_treatment_comments", "wildlife_treatment_vet_attending"):
                val = row.get(c)
                if val and pd.notna(val):
                    pieces.append(str(val))

        if not pieces:
            for c in ("event_details", "event_details__wildlifecarcass_comments"):
                val = row.get(c)
                if val and pd.notna(val):
                    pieces.append(str(val))
                    break

        combined = " — ".join(dict.fromkeys(pieces))
        return shorten(combined, width=shorten_width, placeholder="...") if combined else ""

    def agg_summaries(series: pd.Series, max_unique_local: int = max_unique) -> str:
        unique: List[str] = []
        for s in series.dropna().astype(str):
            s_stripped = s.strip()
            if s_stripped and s_stripped not in unique:
                unique.append(s_stripped)
            if len(unique) >= max_unique_local:
                break
        return "  \n".join(unique)

    df["event_type_mapped"] = df.get("event_type").map(value_map).fillna(df.get("event_type"))
    df["row_summary"] = df.apply(summarize_row, axis=1)

    grouped = (
        df.groupby("event_type_mapped")
        .agg(
            records=("event_type_mapped", "size"),
            summary_details=("row_summary", lambda s: agg_summaries(s, max_unique_local=max_unique)),
        )
        .reset_index()
        .rename(columns={"event_type_mapped": "event_type"})
    )
    if order:
        grouped["event_type"] = pd.Categorical(grouped["event_type"], categories=list(order), ordered=True)
        grouped = grouped.sort_values("event_type").reset_index(drop=True)
    else:
        grouped = grouped.sort_values("records", ascending=False).reset_index(drop=True)

    return grouped
