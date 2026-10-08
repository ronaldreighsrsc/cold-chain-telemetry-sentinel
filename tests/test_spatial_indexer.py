"""
Tests for Uber H3 spatial indexing and distance metrics.
"""

from cold_chain_telemetry_sentinel.engines.spatial_indexer import SpatialIndexer


def test_lat_lng_to_h3_format():
    lat = -33.4150
    lon = -70.7100
    h3_idx = SpatialIndexer.lat_lng_to_h3(lat, lon, resolution=8)
    assert isinstance(h3_idx, str)
    assert len(h3_idx) > 10
    # Deterministic output for same coordinates
    h3_idx2 = SpatialIndexer.lat_lng_to_h3(lat, lon, resolution=8)
    assert h3_idx == h3_idx2


def test_k_ring_neighbors():
    lat = -33.4150
    lon = -70.7100
    origin_cell = SpatialIndexer.lat_lng_to_h3(lat, lon, resolution=8)
    ring = SpatialIndexer.get_k_ring(origin_cell, k=1)
    assert origin_cell in ring
    assert len(ring) >= 7  # Center cell + 6 hexagon neighbors


def test_haversine_distance():
    # Santiago to Valparaíso is approximately 95-120 km
    santiago = (-33.4489, -70.6693)
    valparaiso = (-33.0472, -71.6127)
    dist = SpatialIndexer.haversine_distance_km(santiago[0], santiago[1], valparaiso[0], valparaiso[1])
    assert 90.0 < dist < 120.0


def test_estimate_eta_with_delay():
    # 50 km distance at 50 km/h = 60 minutes + 25 minutes delay = 85 minutes
    eta = SpatialIndexer.estimate_eta_minutes(
        current_lat=-33.40,
        current_lon=-70.70,
        dest_lat=-33.40,
        dest_lon=-70.10,  # ~55 km east
        current_speed_kmh=50.0,
        delay_penalty_minutes=25.0,
    )
    assert eta > 75.0
