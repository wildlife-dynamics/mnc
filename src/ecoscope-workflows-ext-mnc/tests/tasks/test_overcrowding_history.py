import math

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString, Point

from ecoscope_workflows_ext_mnc.tasks.overcrowding._cluster_history import (
    _positions_on_segments,
    detect_vehicle_clusters_over_time,
    episode_vehicle_minutes,
    group_cluster_episodes,
)

ORIGIN_LON, ORIGIN_LAT = 35.1, -1.3
M_PER_DEG_LAT = 110_574
M_PER_DEG_LON = 111_320 * math.cos(math.radians(ORIGIN_LAT))
# 03:00 in Nairobi (UTC+3).
NIGHT = pd.Timestamp("2026-10-05 00:00", tz="UTC")


def _point(east_m: float, north_m: float) -> Point:
    return Point(ORIGIN_LON + east_m / M_PER_DEG_LON, ORIGIN_LAT + north_m / M_PER_DEG_LAT)


def _trajectories(rows, max_time_minutes=30):
    """rows: (subject, minutes after NIGHT, east_m, north_m) fixes, linked into segments
    the way relocations_to_trajectory does (with its `extra__name` column). Segments longer
    than `max_time_minutes` are marked junk, as trajectory_segment_filter's max_time_secs does."""
    fixes = pd.DataFrame(rows, columns=["subject", "minute", "east", "north"]).sort_values(["subject", "minute"])
    segments = []
    for subject, f in fixes.groupby("subject"):
        f = f.to_numpy()
        for (_, m0, e0, n0), (_, m1, e1, n1) in zip(f[:-1], f[1:]):
            dist = math.hypot(e1 - e0, n1 - n0)
            segments.append(
                {
                    "groupby_col": subject,
                    "extra__name": f"Car {subject}",
                    "segment_start": NIGHT + pd.Timedelta(minutes=m0),
                    "segment_end": NIGHT + pd.Timedelta(minutes=m1),
                    "dist_meters": dist,
                    "speed_kmhr": dist / ((m1 - m0) * 60) * 3.6,
                    "junk_status": max_time_minutes is not None and m1 - m0 > max_time_minutes,
                    "geometry": LineString([_point(e0, n0), _point(e1, n1)]),
                }
            )
    return gpd.GeoDataFrame(segments, geometry="geometry", crs=4326)


def _parked(subjects, start, end, every=5, at=(0, 0), spread=20):
    """Vehicles parked around `at`, reporting every `every` minutes from `start` to `end`."""
    rows = []
    for k, s in enumerate(subjects):
        angle = 2 * math.pi * k / len(subjects)
        e, n = at[0] + spread * math.cos(angle), at[1] + spread * math.sin(angle)
        rows += [(s, m, e, n) for m in range(start, end + 1, every)]
    return rows


SIX = ["a", "b", "c", "d", "e", "f"]


class TestPositionsOnSegments:
    def _one_segment(self, grid_s):
        return _positions_on_segments(
            starts_ns=np.array([0], dtype=np.int64),
            ends_ns=np.array([600], dtype=np.int64) * 10**9,
            start_xy=np.array([[0.0, 0.0]]),
            end_xy=np.array([[1000.0, 0.0]]),
            speeds=np.array([6.0]),
            grid_ns=np.array(grid_s, dtype=np.int64) * 10**9,
        )

    def test_interpolates_along_segment_with_its_speed(self):
        valid, pos, speed = self._one_segment([300])
        assert valid.tolist() == [True]
        assert pos[0] == pytest.approx([500.0, 0.0])
        assert speed[0] == 6.0

    def test_times_outside_segments_dropped(self):
        valid, _, _ = self._one_segment([-60, 0, 600, 660])
        assert valid.tolist() == [False, True, True, False]


class TestDetectVehicleClustersOverTime:
    def test_finds_cluster_in_the_middle_of_the_night(self):
        rows = _parked(SIX, 0, 30)
        # They then drive off in different directions; the 90-minute segments are too long to use.
        rows += [(s, 120, 5000 * (k + 1), 0) for k, s in enumerate(SIX)]
        clusters = detect_vehicle_clusters_over_time(_trajectories(rows), interval_minutes=5)
        assert not clusters.empty
        assert (clusters["vehicle_count"] == 6).all()
        assert clusters["snapshot_time"].min() == NIGHT
        assert clusters["snapshot_time"].max() < NIGHT + pd.Timedelta(minutes=60)
        assert clusters.iloc[0]["subject_ids"] == SIX
        assert clusters.crs.to_epsg() == 4326

    def test_vehicle_without_segments_not_counted(self):
        # A single fix gives no segment, so "f" is never placed.
        rows = _parked(SIX[:5], 0, 60) + [("f", 0, 0, 0)]
        assert detect_vehicle_clusters_over_time(_trajectories(rows), interval_minutes=5).empty

    def test_long_segments_left_to_the_segment_filter(self):
        # "f" reports at 0 and 60 only. The task uses that segment as given; the
        # segment filter's max_time_secs is what drops it.
        rows = _parked(SIX[:5], 0, 60) + [("f", 0, 0, 0), ("f", 60, 0, 0)]
        unfiltered = _trajectories(rows, max_time_minutes=None)
        assert not detect_vehicle_clusters_over_time(unfiltered, interval_minutes=5).empty
        filtered = _trajectories(rows, max_time_minutes=30)
        assert detect_vehicle_clusters_over_time(filtered, interval_minutes=5).empty

    def test_only_vehicles_inside_aoi_count(self):
        # One crowd at a sighting, and an operator's fleet parked in a yard ~180 km east.
        nairobi_yard = (180_000, 0)
        rows = _parked(SIX, 0, 30) + _parked(list("uvwxyz"), 0, 30, at=nairobi_yard)
        tr = _trajectories(rows)
        conservancy = gpd.GeoDataFrame(geometry=[_point(0, 0).buffer(0.05)], crs=4326)  # ~5.5 km radius

        everywhere = detect_vehicle_clusters_over_time(tr, interval_minutes=5)
        assert everywhere.groupby("snapshot_time").size().max() == 2

        inside = detect_vehicle_clusters_over_time(tr, interval_minutes=5, aoi=conservancy)
        assert not inside.empty
        assert all(set(ids) == set(SIX) for ids in inside["subject_ids"])
        assert inside.crs.to_epsg() == 4326

    def test_junk_segments_dropped(self):
        tr = _trajectories(_parked(SIX, 0, 30))
        tr.loc[tr["groupby_col"] == "f", "junk_status"] = True
        assert detect_vehicle_clusters_over_time(tr, interval_minutes=5).empty

    def test_vehicles_driving_past_ignored(self):
        rows = _parked(SIX[:5], 0, 30)
        # Sixth vehicle passes straight through the sighting at ~36 km/h.
        rows += [("f", 0, -3000, 0), ("f", 10, 3000, 0)]
        found = detect_vehicle_clusters_over_time(_trajectories(rows), interval_minutes=1, max_speed_kmh=None)
        assert not found.empty
        assert detect_vehicle_clusters_over_time(_trajectories(rows), interval_minutes=1, max_speed_kmh=20).empty

    def test_zones_excluded(self):
        lodge = gpd.GeoDataFrame(geometry=[_point(0, 0)], crs=4326)
        rows = _parked(SIX, 0, 30)
        result = detect_vehicle_clusters_over_time(_trajectories(rows), zones=lodge, zone_buffer_meters=100)
        assert result.empty
        assert "snapshot_time" in result.columns

    def test_all_junk_is_empty(self):
        tr = _trajectories(_parked(SIX, 0, 30))
        tr["junk_status"] = True
        result = detect_vehicle_clusters_over_time(tr)
        assert result.empty
        assert "snapshot_time" in result.columns


def _clusters_every_5(rows):
    return detect_vehicle_clusters_over_time(_trajectories(rows), interval_minutes=5)


class TestGroupClusterEpisodes:
    def _episodes(self, rows, **kwargs):
        return group_cluster_episodes(_clusters_every_5(rows), interval_minutes=5, **kwargs)

    def test_one_row_per_incident(self):
        rows = _parked(SIX, 0, 30) + _parked(["g"], 10, 20, at=(10, 0), spread=0)
        episodes = self._episodes(rows)
        assert list(episodes.columns) == [
            "episode_id",
            "start_time",
            "end_time",
            "duration_minutes",
            "peak_vehicle_count",
            "peak_time",
            "vehicle_count",
            "max_spread_m",
            "geometry",
        ]
        assert len(episodes) == 1
        episode = episodes.iloc[0]
        assert episode["start_time"] == NIGHT
        assert episode["duration_minutes"] == 35  # snapshots 0..30, plus the last snapshot's 5 min
        assert episode["peak_vehicle_count"] == 7
        assert episode["peak_time"] == NIGHT + pd.Timedelta(minutes=10)
        assert episode["vehicle_count"] == 7

    def test_short_gap_same_episode_long_gap_new_episode(self):
        rows = _parked(SIX, 0, 20) + _parked(SIX, 30, 50)
        assert len(self._episodes(rows, max_gap_minutes=10)) == 1
        far_later = _parked(SIX, 0, 20) + _parked(SIX, 120, 140)
        assert len(self._episodes(far_later, max_gap_minutes=10)) == 2

    def test_separate_sightings_are_separate_episodes(self):
        rows = _parked(SIX, 0, 30) + _parked(list("uvwxyz"), 0, 30, at=(5000, 0))
        episodes = self._episodes(rows)
        assert len(episodes) == 2
        assert set(episodes["episode_id"]) == {0, 1}

    def test_single_snapshot_lasts_one_interval(self):
        # Parked 0-5 min, checked every 10 min: only the 0 tick sees them.
        clusters = detect_vehicle_clusters_over_time(_trajectories(_parked(SIX, 0, 5)), interval_minutes=10)
        episodes = group_cluster_episodes(clusters, interval_minutes=10)
        assert episodes.iloc[0]["start_time"] == episodes.iloc[0]["end_time"]
        assert episodes.iloc[0]["duration_minutes"] == 10

    def test_min_duration_filter(self):
        rows = _parked(SIX, 0, 5)
        assert len(self._episodes(rows, min_duration_minutes=10)) == 1
        assert self._episodes(rows, min_duration_minutes=15).empty

    def test_empty(self):
        empty = detect_vehicle_clusters_over_time(_trajectories([("a", 0, 0, 0), ("a", 5, 0, 0)]))
        assert group_cluster_episodes(empty).empty


class TestEpisodeVehicleMinutes:
    def test_one_row_per_vehicle_per_incident(self):
        rows = _parked(SIX, 0, 30) + _parked(["g"], 10, 20, at=(10, 0), spread=0)
        vehicles = episode_vehicle_minutes(_clusters_every_5(rows), interval_minutes=5)
        assert list(vehicles.columns) == ["episode_id", "subject_id", "first_seen", "last_seen", "minutes_in_cluster"]
        assert len(vehicles) == 7
        assert (vehicles["episode_id"] == 0).all()
        g = vehicles.set_index("subject_id").loc["g"]
        assert g["first_seen"] == NIGHT + pd.Timedelta(minutes=10)
        assert g["last_seen"] == NIGHT + pd.Timedelta(minutes=20)
        assert g["minutes_in_cluster"] == 15  # seen at 10, 15, 20
        assert vehicles.iloc[-1]["subject_id"] == "g"  # fewest minutes last
        assert (vehicles[vehicles["subject_id"] != "g"]["minutes_in_cluster"] == 35).all()

    def test_episode_ids_match_incident_table(self):
        rows = _parked(SIX, 0, 30) + _parked(list("uvwxyz"), 60, 70, at=(5000, 0))
        clusters = _clusters_every_5(rows)
        episodes = group_cluster_episodes(clusters, interval_minutes=5)
        vehicles = episode_vehicle_minutes(clusters, interval_minutes=5)
        counts = vehicles.groupby("episode_id").size()
        assert counts.to_dict() == episodes.set_index("episode_id")["vehicle_count"].to_dict()
        later = episodes.sort_values("start_time").iloc[-1]["episode_id"]
        assert set(vehicles[vehicles["episode_id"] == later]["subject_id"]) == set("uvwxyz")

    def test_min_duration_drops_the_same_incidents(self):
        rows = _parked(SIX, 0, 30) + _parked(list("uvwxyz"), 60, 65, at=(5000, 0))
        clusters = _clusters_every_5(rows)
        episodes = group_cluster_episodes(clusters, interval_minutes=5, min_duration_minutes=20)
        vehicles = episode_vehicle_minutes(clusters, interval_minutes=5, min_duration_minutes=20)
        assert list(episodes["episode_id"]) == [0]
        assert set(vehicles["subject_id"]) == set(SIX)

    def test_empty(self):
        empty = detect_vehicle_clusters_over_time(_trajectories([("a", 0, 0, 0), ("a", 5, 0, 0)]))
        result = episode_vehicle_minutes(empty)
        assert result.empty
        assert "minutes_in_cluster" in result.columns
