from typing import Annotated

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from pydantic import Field
from pydantic.json_schema import SkipJsonSchema
from wt_registry import register
from ecoscope.platform.annotations import AnyDataFrame, AnyGeoDataFrame

from ._vehicle_clusters import _find_clusters, _metric_crs

SNAPSHOT_CLUSTER_COLUMNS = [
    "snapshot_time",
    "vehicle_count",
    "subject_ids",
    "max_spread_m",
    "geometry",
]
EPISODE_COLUMNS = [
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
EPISODE_VEHICLE_COLUMNS = [
    "episode_id",
    "subject_id",
    "first_seen",
    "last_seen",
    "minutes_in_cluster",
]


def _positions_on_segments(
    starts_ns: np.ndarray,
    ends_ns: np.ndarray,
    start_xy: np.ndarray,
    end_xy: np.ndarray,
    speeds: np.ndarray,
    grid_ns: np.ndarray,
):
    """One vehicle's position and speed at each grid time, from its trajectory segments.

    The vehicle is placed along the segment covering the grid time, in proportion to the
    time elapsed, and takes that segment's speed. Grid times outside every segment (before
    the first fix, after the last, or in a gap left by the segment filter) are dropped.
    """
    k = np.searchsorted(starts_ns, grid_ns, side="right") - 1
    kc = np.clip(k, 0, len(starts_ns) - 1)
    valid = (k >= 0) & (grid_ns <= ends_ns[kc])
    span = (ends_ns[kc] - starts_ns[kc]).astype(float)
    frac = np.where(span > 0, (grid_ns - starts_ns[kc]) / np.where(span > 0, span, 1), 0.0)
    pos = start_xy[kc] + (end_xy[kc] - start_xy[kc]) * frac[:, None]
    return valid, pos, speeds[kc]


@register()
def detect_vehicle_clusters_over_time(
    trajectories: Annotated[
        AnyGeoDataFrame,
        Field(
            description="Vehicle trajectory segments for the whole period (from relocations_to_trajectory).",
            exclude=True,
        ),
    ],
    interval_minutes: Annotated[
        float,
        Field(description="Check for clusters every this many minutes across the whole time range.", gt=0),
    ] = 2,
    radius_meters: Annotated[
        float,
        Field(
            description="A cluster forms when enough vehicles are within this distance of one vehicle.",
            gt=0,
        ),
    ] = 50,
    min_vehicles: Annotated[
        int,
        Field(description="Vehicles (including the centre vehicle) needed to count as a cluster.", ge=2),
    ] = 6,
    max_speed_kmh: Annotated[
        float | SkipJsonSchema[None],
        Field(description="Ignore vehicles moving faster than this, e.g. convoys driving past."),
    ] = 50,
    aoi: Annotated[
        AnyGeoDataFrame | SkipJsonSchema[None],
        Field(
            description="Only count vehicles inside this area, e.g. the conservancy boundary. "
            "Without it, vehicles parked together anywhere (a Nairobi yard) can form clusters."
        ),
    ] = None,
    zones: Annotated[
        AnyGeoDataFrame | SkipJsonSchema[None],
        Field(description="Places vehicles legitimately gather: lodges, sundowner spots, villages, bush breakfasts."),
    ] = None,
    zone_buffer_meters: Annotated[
        float,
        Field(description="Distance around each zone that is also excluded. Needed when zones are points.", ge=0),
    ] = 0,
) -> AnyGeoDataFrame:
    """Replay the whole period and find vehicle clusters at regular snapshots.

    At every `interval_minutes`, each vehicle is placed along the trajectory segment it was
    on at that moment and given that segment's speed. Vehicles outside the `aoi`, inside
    exclusion zones or moving faster than `max_speed_kmh` are dropped, and the rest are
    clustered (DBSCAN-like; see `_find_clusters`). Returns one row per cluster per snapshot (times in UTC);
    pass the result to `group_cluster_episodes` to get one row per crowding incident.

    Which segments are usable is decided by the trajectory segment filter; segments it
    removes leave the vehicle unaccounted for. Recommended settings:

    - `max_time_secs` around 1800. Positions along longer segments are a guess (a 4-hour
      gap becomes a slow straight line), which can place vehicles where they never were
      and make moving vehicles look parked.
    - `min_length_meters` and `min_speed_kmhr` of 0. Anything above 0 drops segments where
      a parked vehicle didn't move, so vehicles parked at a sighting would disappear.
    """
    segs = trajectories
    if "junk_status" in segs.columns:
        segs = segs[~segs["junk_status"].astype(bool)]
    if segs.empty:
        return gpd.GeoDataFrame(columns=SNAPSHOT_CLUSTER_COLUMNS, geometry="geometry", crs=trajectories.crs)

    segs = segs.assign(
        segment_start=pd.to_datetime(segs["segment_start"], utc=True),
        segment_end=pd.to_datetime(segs["segment_end"], utc=True),
    ).sort_values(["groupby_col", "segment_start"])
    has_aoi = aoi is not None and not aoi.empty
    # Pick the UTM zone from the AOI when there is one, so vehicles that also travel far away don't shift it.
    metric_crs = _metric_crs(aoi if has_aoi else segs)
    lines = segs.geometry.to_crs(metric_crs).to_numpy()
    start_xy = shapely.get_coordinates(shapely.get_point(lines, 0))
    end_xy = shapely.get_coordinates(shapely.get_point(lines, -1))
    starts_ns = segs["segment_start"].dt.as_unit("ns").astype(np.int64).to_numpy()
    ends_ns = segs["segment_end"].dt.as_unit("ns").astype(np.int64).to_numpy()
    speeds = segs["speed_kmhr"].to_numpy(dtype=float)

    interval = pd.Timedelta(minutes=interval_minutes)
    grid = pd.date_range(segs["segment_start"].min().ceil(interval), segs["segment_end"].max(), freq=interval)
    grid_ns = grid.asi8

    frames = []
    for subject_id, idx in segs.groupby(["groupby_col"], sort=False).indices.items():
        valid, pos, speed = _positions_on_segments(
            starts_ns[idx], ends_ns[idx], start_xy[idx], end_xy[idx], speeds[idx], grid_ns
        )
        if valid.any():
            frames.append(
                pd.DataFrame(
                    {
                        "snapshot_time": grid_ns[valid],
                        "groupby_col": str(subject_id),
                        "x": pos[valid, 0],
                        "y": pos[valid, 1],
                        "speed_kmh": speed[valid],
                    }
                )
            )
    if not frames:
        return gpd.GeoDataFrame(columns=SNAPSHOT_CLUSTER_COLUMNS, geometry="geometry", crs=trajectories.crs)
    positions = pd.concat(frames, ignore_index=True)

    keep = np.ones(len(positions), dtype=bool)
    points = shapely.points(positions[["x", "y"]].to_numpy())

    if max_speed_kmh is not None:
        keep &= (positions["speed_kmh"] <= max_speed_kmh).to_numpy()
    if has_aoi:
        area = shapely.union_all(aoi.geometry.to_crs(metric_crs).to_numpy())
        shapely.prepare(area)
        keep &= shapely.intersects(points, area)
    if zones is not None and not zones.empty:
        zone_geoms = zones.geometry.to_crs(metric_crs)
        if zone_buffer_meters > 0:
            zone_geoms = zone_geoms.buffer(zone_buffer_meters)
        excluded_area = shapely.union_all(zone_geoms.to_numpy())
        shapely.prepare(excluded_area)
        keep &= ~shapely.intersects(points, excluded_area)
    positions = positions[keep]

    rows = []
    for snapshot_ns, snap in positions.groupby("snapshot_time", sort=True):
        if len(snap) < min_vehicles:
            continue
        xy = snap[["x", "y"]].to_numpy()
        found, dist = _find_clusters(xy, radius_meters, min_vehicles)
        for members in found:
            rows.append(
                {
                    "snapshot_time": pd.Timestamp(snapshot_ns, tz="UTC"),
                    "vehicle_count": len(members),
                    "subject_ids": sorted(snap["groupby_col"].iloc[members]),
                    "max_spread_m": round(float(dist[np.ix_(members, members)].max()), 1),
                    "geometry": shapely.Point(xy[members].mean(axis=0)),
                }
            )
    if not rows:
        return gpd.GeoDataFrame(columns=SNAPSHOT_CLUSTER_COLUMNS, geometry="geometry", crs=trajectories.crs)
    clusters = gpd.GeoDataFrame(rows, geometry="geometry", crs=metric_crs).to_crs(trajectories.crs)
    return clusters[SNAPSHOT_CLUSTER_COLUMNS]


def _build_episodes(
    snapshot_clusters: gpd.GeoDataFrame,
    interval_minutes: float,
    max_gap_minutes: float,
    min_duration_minutes: float,
) -> list[dict]:
    """Join clusters at consecutive snapshots into incidents, numbered by start time.

    A cluster continues an incident when it shares at least one vehicle with the
    incident's previous snapshot, no more than `max_gap_minutes` earlier. Each snapshot
    stands for one interval, so an incident's duration is end - start plus one interval.
    """
    max_gap = pd.Timedelta(minutes=max_gap_minutes)
    open_episodes: list[dict] = []
    episodes: list[dict] = []
    ordered = snapshot_clusters.sort_values("snapshot_time", kind="stable")
    for snapshot_time, snap in ordered.groupby("snapshot_time", sort=True):
        still_open = []
        for episode in open_episodes:
            (still_open if snapshot_time - episode["end_time"] <= max_gap else episodes).append(episode)
        open_episodes = still_open

        extended: set[int] = set()
        for _, cluster in snap.iterrows():
            members = set(cluster["subject_ids"])
            candidates = [
                (len(members & ep["last_members"]), i)
                for i, ep in enumerate(open_episodes)
                if i not in extended and members & ep["last_members"]
            ]
            if candidates:
                _, i = max(candidates)
                episode = open_episodes[i]
                extended.add(i)
            else:
                episode = {
                    "start_time": snapshot_time,
                    "peak_vehicle_count": 0,
                    "vehicles": {},
                    "points": [],
                    "max_spread_m": 0.0,
                }
                open_episodes.append(episode)
                extended.add(len(open_episodes) - 1)
            episode["end_time"] = snapshot_time
            episode["last_members"] = members
            episode["points"].append(cluster.geometry)
            episode["max_spread_m"] = max(episode["max_spread_m"], float(cluster["max_spread_m"]))
            if cluster["vehicle_count"] > episode["peak_vehicle_count"]:
                episode["peak_vehicle_count"] = int(cluster["vehicle_count"])
                episode["peak_time"] = snapshot_time
            for subject_id in cluster["subject_ids"]:
                vehicle = episode["vehicles"].setdefault(subject_id, {"first_seen": snapshot_time, "minutes": 0.0})
                vehicle["last_seen"] = snapshot_time
                vehicle["minutes"] += interval_minutes
    episodes.extend(open_episodes)

    for episode in episodes:
        episode["duration_minutes"] = (
            episode["end_time"] - episode["start_time"]
        ).total_seconds() / 60 + interval_minutes
    episodes = [e for e in episodes if e["duration_minutes"] >= min_duration_minutes]
    episodes.sort(key=lambda e: e["start_time"])
    for episode_id, episode in enumerate(episodes):
        episode["episode_id"] = episode_id
    return episodes


IntervalMinutes = Annotated[
    float,
    Field(description="The interval_minutes used in detect_vehicle_clusters_over_time.", gt=0),
]
MaxGapMinutes = Annotated[
    float,
    Field(
        description="A cluster that disappears for up to this long and comes back is still the same incident.",
        ge=0,
    ),
]
MinDurationMinutes = Annotated[
    float,
    Field(description="Drop incidents shorter than this (an incident seen at one snapshot lasts one interval).", ge=0),
]


@register()
def group_cluster_episodes(
    snapshot_clusters: Annotated[
        AnyGeoDataFrame,
        Field(description="Output of detect_vehicle_clusters_over_time.", exclude=True),
    ],
    interval_minutes: IntervalMinutes = 2,
    max_gap_minutes: MaxGapMinutes = 10,
    min_duration_minutes: MinDurationMinutes = 0,
) -> AnyGeoDataFrame:
    """One row per crowding incident: when, where, how long and how many vehicles.

    Clusters at consecutive snapshots that share a vehicle are one incident (see
    `_build_episodes`). `interval_minutes` must match the value used to detect the
    clusters. Use `episode_vehicle_minutes` with the same settings for who was there;
    the `episode_id`s match.
    """
    episodes = _build_episodes(snapshot_clusters, interval_minutes, max_gap_minutes, min_duration_minutes)
    if not episodes:
        return gpd.GeoDataFrame(columns=EPISODE_COLUMNS, geometry="geometry", crs=snapshot_clusters.crs)
    rows = [
        {
            "episode_id": e["episode_id"],
            "start_time": e["start_time"],
            "end_time": e["end_time"],
            "duration_minutes": e["duration_minutes"],
            "peak_vehicle_count": e["peak_vehicle_count"],
            "peak_time": e["peak_time"],
            "vehicle_count": len(e["vehicles"]),
            "max_spread_m": e["max_spread_m"],
            "geometry": shapely.Point(np.mean([p.x for p in e["points"]]), np.mean([p.y for p in e["points"]])),
        }
        for e in episodes
    ]
    return gpd.GeoDataFrame(rows, columns=EPISODE_COLUMNS, geometry="geometry", crs=snapshot_clusters.crs)


@register()
def episode_vehicle_minutes(
    snapshot_clusters: Annotated[
        AnyGeoDataFrame,
        Field(description="Output of detect_vehicle_clusters_over_time.", exclude=True),
    ],
    interval_minutes: IntervalMinutes = 2,
    max_gap_minutes: MaxGapMinutes = 10,
    min_duration_minutes: MinDurationMinutes = 0,
) -> AnyDataFrame:
    """One row per vehicle per crowding incident: when it was first and last seen in the
    cluster, and for how many minutes (snapshots present x `interval_minutes`).

    The basis for dwell-time rules. Use the same settings as `group_cluster_episodes` so
    the `episode_id`s match. Minutes count only snapshots where the crowd met the cluster
    rule, so a vehicle still at the sighting while the crowd briefly thinned isn't credited.
    """
    episodes = _build_episodes(snapshot_clusters, interval_minutes, max_gap_minutes, min_duration_minutes)
    rows = [
        {
            "episode_id": e["episode_id"],
            "subject_id": subject_id,
            "first_seen": v["first_seen"],
            "last_seen": v["last_seen"],
            "minutes_in_cluster": v["minutes"],
        }
        for e in episodes
        for subject_id, v in e["vehicles"].items()
    ]
    result = pd.DataFrame(rows, columns=EPISODE_VEHICLE_COLUMNS)
    return result.sort_values(
        ["episode_id", "minutes_in_cluster", "subject_id"], ascending=[True, False, True], kind="stable"
    ).reset_index(drop=True)
