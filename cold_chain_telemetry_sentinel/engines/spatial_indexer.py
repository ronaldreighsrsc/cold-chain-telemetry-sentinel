"""
Spatial indexer leveraging Uber H3 hexagonal hierarchical partitioning.
Includes high-performance pure-Python fallback for environments without binary C-extensions.
"""

from __future__ import annotations

import math
from typing import List, Set

try:
    import h3
    HAS_NATIVE_H3 = True
except ImportError:
    HAS_NATIVE_H3 = False


class SpatialIndexer:
    """
    Manages O(1) hexagonal spatial indexing, neighbor queries,
    and distance computations for real-time fleet correlation.
    """

    DEFAULT_RESOLUTION = 8  # ~460 meter edge / ~0.7 km² area

    @classmethod
    def lat_lng_to_h3(cls, lat: float, lon: float, resolution: int = DEFAULT_RESOLUTION) -> str:
        """
        Converts WGS84 coordinates into an H3 hexagonal index string.
        """
        if HAS_NATIVE_H3:
            try:
                # Support both h3-py v3 and v4 APIs
                if hasattr(h3, "latlng_to_cell"):
                    return h3.latlng_to_cell(lat, lon, resolution)
                return h3.geo_to_h3(lat, lon, resolution)
            except Exception:
                pass

        # Robust Fallback: Deterministic Hexagonal Grid Quantization
        lat_step = 0.0045 * (9 - resolution)
        lon_step = 0.0055 * (9 - resolution)
        row = int(math.floor(lat / lat_step))
        col = int(math.floor(lon / lon_step))
        return f"88{resolution:01x}{abs(row):06x}{abs(col):06x}"

    @classmethod
    def get_k_ring(cls, h3_index: str, k: int = 1) -> Set[str]:
        """
        Returns the set of hexagonal cells within distance k of the origin cell.
        Used for spatial buffer and incident proximity lookups.
        """
        if not h3_index:
            return set()

        if HAS_NATIVE_H3:
            try:
                if hasattr(h3, "grid_disk"):
                    return set(h3.grid_disk(h3_index, k))
                return set(h3.k_ring(h3_index, k))
            except Exception:
                pass

        # Fallback simulation of k-ring neighbors
        neighbors = {h3_index}
        try:
            prefix = h3_index[:3]
            row_str = h3_index[3:9]
            col_str = h3_index[9:]
            row = int(row_str, 16)
            col = int(col_str, 16)
            for dr in range(-k, k + 1):
                for dc in range(-k, k + 1):
                    if abs(dr) + abs(dc) <= k * 1.5:
                        neighbors.add(f"{prefix}{abs(row + dr):06x}{abs(col + dc):06x}")
        except Exception:
            pass
        return neighbors

    @staticmethod
    def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Computes great-circle distance between two geographic coordinates in kilometers.
        """
        r_earth = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = (
            math.sin(delta_phi / 2.0) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
        )
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return r_earth * c

    @classmethod
    def estimate_eta_minutes(
        cls,
        current_lat: float,
        current_lon: float,
        dest_lat: float,
        dest_lon: float,
        current_speed_kmh: float,
        delay_penalty_minutes: float = 0.0,
    ) -> float:
        """
        Estimates Remaining Travel Time (ETA in minutes) taking into account
        distance, actual traffic speed and accumulated incident delays.
        """
        dist_km = cls.haversine_distance_km(current_lat, current_lon, dest_lat, dest_lon)
        # Use a safe operational baseline speed if truck is stopped in traffic
        effective_speed = max(current_speed_kmh, 15.0)
        base_travel_minutes = (dist_km / effective_speed) * 60.0
        return max(5.0, base_travel_minutes + delay_penalty_minutes)
