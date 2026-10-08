"""
High-Fidelity Telemetry and Spatial Incident Simulator.
Generates realistic multi-vehicle urban transit trajectories, sensor noise,
dynamic traffic choke-points, and refrigeration hardware anomalies.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Tuple

from cold_chain_telemetry_sentinel.domain.models import (
    CargoProfile,
    CargoType,
    CompressorStatus,
    IncidentSeverity,
)
from cold_chain_telemetry_sentinel.engines.spatial_indexer import SpatialIndexer


# Realistic Metropolian Waypoints (Santiago Logistics Corridors)
SANTIAGO_LOGISTICS_ROUTES = {
    "ROUTE_AIRPORT_TO_EAST": [
        (-33.3930, -70.7930),  # Santiago International Cargo Terminal (Pudahuel)
        (-33.4050, -70.7500),  # Costanera Norte West
        (-33.4150, -70.7100),  # Renca Distribution Corridor
        (-33.4200, -70.6600),  # Costanera Central / Mapocho
        (-33.4160, -70.6050),  # Providencia Medical Hub
        (-33.4080, -70.5650),  # Las Condes Regional Cold Vault
    ],
    "ROUTE_NORTH_TO_SOUTH": [
        (-33.3600, -70.7000),  # Quilicura Industrial Hub
        (-33.4000, -70.6800),  # Autopista Central North
        (-33.4450, -70.6600),  # Santiago Centro Arterial
        (-33.4900, -70.6650),  # San Miguel Corridor
        (-33.5500, -70.6800),  # San Bernardo Intermodal Cold Warehouse
    ],
    "ROUTE_WEST_PORT_CONNECTOR": [
        (-33.4500, -70.8200),  # Route 68 Logistics Gateway
        (-33.4550, -70.7500),  # Maipu Industrial Center
        (-33.4600, -70.6800),  # Cerrillos Depot
        (-33.4800, -70.6200),  # Macul Bio-Pharmaceutical Research Campus
    ],
}


class FleetSimulator:
    """
    Generates realistic real-time streaming telemetry and incident shocks.
    """

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.vehicles = [
            {
                "vehicle_id": "TRUCK-VAX-01",
                "route_key": "ROUTE_AIRPORT_TO_EAST",
                "cargo_profile": CargoProfile.default_vaccines(),
                "initial_cargo_temp": 3.5,
                "compressor_status": CompressorStatus.OPTIMAL,
            },
            {
                "vehicle_id": "TRUCK-PHARMA-02",
                "route_key": "ROUTE_NORTH_TO_SOUTH",
                "cargo_profile": CargoProfile.default_vaccines(),
                "initial_cargo_temp": 4.2,
                "compressor_status": CompressorStatus.DEGRADED,  # Hardware stress case
            },
            {
                "vehicle_id": "TRUCK-SEAFOOD-03",
                "route_key": "ROUTE_WEST_PORT_CONNECTOR",
                "cargo_profile": CargoProfile.default_frozen(),
                "initial_cargo_temp": -22.0,
                "compressor_status": CompressorStatus.OPTIMAL,
            },
            {
                "vehicle_id": "TRUCK-DAIRY-04",
                "route_key": "ROUTE_AIRPORT_TO_EAST",
                "cargo_profile": CargoProfile.default_dairy(),
                "initial_cargo_temp": 2.5,
                "compressor_status": CompressorStatus.OPTIMAL,
            },
        ]

    def generate_incident_shocks(self) -> List[Dict[str, Any]]:
        """
        Creates synthetic high-impact road disruptions along key transit arteries.
        """
        now = datetime.now(timezone.utc)
        return [
            {
                "incident_id": "INC-COSTANERA-GRIDLOCK",
                "timestamp": now.isoformat(),
                "latitude": -33.4150,
                "longitude": -70.7100,
                "severity": IncidentSeverity.CRITICAL_GRIDLOCK.value,
                "delay_impact_minutes": 45.0,
                "description": "Multi-vehicle collision blocking 3 lanes on Costanera Norte corridor.",
                "h3_index": SpatialIndexer.lat_lng_to_h3(-33.4150, -70.7100),
                "active": True,
            },
            {
                "incident_id": "INC-QUILICURA-ROADWORK",
                "timestamp": now.isoformat(),
                "latitude": -33.4000,
                "longitude": -70.6800,
                "severity": IncidentSeverity.HIGH.value,
                "delay_impact_minutes": 30.0,
                "description": "Emergency asphalt repair near Autopista Central North entrance.",
                "h3_index": SpatialIndexer.lat_lng_to_h3(-33.4000, -70.6800),
                "active": True,
            },
        ]

    def generate_trip_telemetry_stream(
        self,
        steps_per_route: int = 20,
        inject_failure_on_step: int = 10,
        failure_vehicle_id: str = "TRUCK-PHARMA-02",
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yields discrete sequential telemetry frames simulating a live vehicle fleet.
        """
        base_time = datetime.now(timezone.utc)

        for step in range(steps_per_route):
            progress_ratio = step / max(1, steps_per_route - 1)
            t_now = base_time + timedelta(minutes=step * 2)

            for v in self.vehicles:
                v_id = v["vehicle_id"]
                waypoints = SANTIAGO_LOGISTICS_ROUTES[v["route_key"]]

                # Interpolate GPS along route
                idx_float = progress_ratio * (len(waypoints) - 1)
                idx_low = int(idx_float)
                idx_high = min(idx_low + 1, len(waypoints) - 1)
                sub_ratio = idx_float - idx_low

                p1 = waypoints[idx_low]
                p2 = waypoints[idx_high]

                lat = p1[0] + (p2[0] - p1[0]) * sub_ratio + self.rng.uniform(-0.0008, 0.0008)
                lon = p1[1] + (p2[1] - p1[1]) * sub_ratio + self.rng.uniform(-0.0008, 0.0008)

                ambient_temp = 28.0 + self.rng.uniform(-1.5, 2.0)  # Summer afternoon in Santiago

                comp_status = v["compressor_status"]
                cargo_temp = v["initial_cargo_temp"]

                # Simulate thermal degradation if failure injected
                if v_id == failure_vehicle_id and step >= inject_failure_on_step:
                    comp_status = CompressorStatus.FAILURE
                    # Cargo warming up rapidly
                    cargo_temp += (step - inject_failure_on_step + 1) * 0.55
                elif comp_status == CompressorStatus.DEGRADED:
                    cargo_temp += 0.08 * step
                else:
                    cargo_temp += self.rng.uniform(-0.05, 0.05)

                speed = 65.0 if step < 5 else (18.0 if step < 14 else 45.0)

                payload = {
                    "vehicle_id": v_id,
                    "timestamp": t_now.isoformat(),
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "speed_kmh": round(speed + self.rng.uniform(-3, 3), 1),
                    "ambient_temp_c": round(ambient_temp, 1),
                    "cargo_temp_c": round(cargo_temp, 2),
                    "compressor_status": comp_status.value,
                    "battery_soc_pct": round(max(20.0, 98.0 - step * 1.5), 1),
                    "h3_index": SpatialIndexer.lat_lng_to_h3(lat, lon),
                }

                yield payload
