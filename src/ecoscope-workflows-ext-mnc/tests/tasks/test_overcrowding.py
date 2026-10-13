import math

import numpy as np

from ecoscope_workflows_ext_mnc.tasks.overcrowding._vehicle_clusters import _find_clusters


def _ring(n, radius_m, centre=(0, 0)):
    return [
        (centre[0] + radius_m * math.cos(2 * math.pi * k / n), centre[1] + radius_m * math.sin(2 * math.pi * k / n))
        for k in range(n)
    ]


def _clusters(points, radius_meters=50, min_vehicles=6):
    found, dist = _find_clusters(np.array(points, dtype=float), radius_meters, min_vehicles)
    return sorted((sorted(map(int, c)) for c in found), key=len, reverse=True), dist


class TestFindClusters:
    def test_six_vehicles_at_a_sighting(self):
        points = _ring(5, 40) + [(0, 0)] + [(2000, 2000)]
        clusters, dist = _clusters(points)
        assert clusters == [[0, 1, 2, 3, 4, 5]]
        assert dist[np.ix_(clusters[0], clusters[0])].max() <= 100

    def test_five_vehicles_is_not_a_cluster(self):
        clusters, _ = _clusters(_ring(4, 30) + [(0, 0)])
        assert clusters == []

    def test_road_convoy_does_not_chain(self):
        # 10 vehicles 40 m apart in a line: each has at most 3 within 50 m.
        clusters, _ = _clusters([(40 * i, 0) for i in range(10)])
        assert clusters == []

    def test_two_separate_sightings(self):
        points = _ring(5, 30) + [(0, 0)] + _ring(7, 30, centre=(3000, 0)) + [(3000, 0)]
        clusters, _ = _clusters(points)
        assert [len(c) for c in clusters] == [8, 6]

    def test_overlapping_discs_merge(self):
        # Two dense groups 60 m apart share the vehicles between them.
        points = _ring(5, 20) + [(0, 0)] + _ring(5, 20, centre=(60, 0)) + [(60, 0), (30, 0)]
        clusters, _ = _clusters(points)
        assert [len(c) for c in clusters] == [13]
