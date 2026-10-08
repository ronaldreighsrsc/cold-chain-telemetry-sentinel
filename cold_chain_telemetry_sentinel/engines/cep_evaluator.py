"""
Complex Event Processing (CEP) engine.
Correlates high-velocity telemetry streams with spatial traffic disruptions,
computes sliding-window aggregates, and triggers multi-tier operational alerts.
"""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from cold_chain_telemetry_sentinel.domain.models import (
    AlertLevel,
    CargoProfile,
    CompressorStatus,
    OperationalAlert,
    TelemetryRecord,
    TrafficIncident,
    VehicleState,
)
from cold_chain_telemetry_sentinel.domain.thermal_model import ThermalDynamicsEngine
from cold_chain_telemetry_sentinel.engines.spatial_indexer import SpatialIndexer


class ComplexEventEvaluator:
    """
    Stateful Complex Event Processing (CEP) rule engine.
    Detects composite temporal-spatial conditions that threaten cold-chain cargo.
    """

    def __init__(self, window_size: int = 15):
        self.window_size = window_size
        # vehicle_id -> deque of recent telemetry pings
        self._sliding_windows: Dict[str, deque[TelemetryRecord]] = defaultdict(
            lambda: deque(maxlen=self.window_size)
        )
        # H3 index -> active TrafficIncident
        self._active_incidents_by_h3: Dict[str, TrafficIncident] = {}
        # vehicle_id -> VehicleState
        self._fleet_states: Dict[str, VehicleState] = {}
        self._alert_history: List[OperationalAlert] = []

    def register_vehicle(
        self,
        vehicle_id: str,
        cargo_profile: CargoProfile,
        destination_lat: float,
        destination_lon: float,
        base_eta_minutes: float = 60.0,
    ) -> VehicleState:
        """
        Initializes state tracking for an active refrigerated vehicle.
        """
        state = VehicleState(
            vehicle_id=vehicle_id,
            cargo_profile=cargo_profile,
            destination_lat=destination_lat,
            destination_lon=destination_lon,
            base_eta_minutes=base_eta_minutes,
            current_eta_minutes=base_eta_minutes,
        )
        self._fleet_states[vehicle_id] = state
        return state

    def ingest_incident(self, incident: TrafficIncident) -> None:
        """
        Registers an active traffic incident and updates the spatial disruption index.
        """
        if not incident.h3_index:
            incident.h3_index = SpatialIndexer.lat_lng_to_h3(incident.latitude, incident.longitude)

        if incident.active:
            self._active_incidents_by_h3[incident.h3_index] = incident
        else:
            self._active_incidents_by_h3.pop(incident.h3_index, None)

    def remove_incident(self, incident_id: str) -> None:
        """Removes a cleared incident from active index."""
        to_remove = [h3 for h3, inc in self._active_incidents_by_h3.items() if inc.incident_id == incident_id]
        for h3 in to_remove:
            self._active_incidents_by_h3.pop(h3, None)

    def process_telemetry(self, telemetry: TelemetryRecord) -> Optional[OperationalAlert]:
        """
        Ingests a telemetry ping, correlates with spatial incidents,
        updates thermal projections, and generates alerts if thresholds are breached.
        """
        v_id = telemetry.vehicle_id
        if v_id not in self._fleet_states:
            # Default to pharma vaccine profile if unmapped
            self.register_vehicle(
                vehicle_id=v_id,
                cargo_profile=CargoProfile.default_vaccines(),
                destination_lat=telemetry.latitude + 0.05,
                destination_lon=telemetry.longitude + 0.05,
            )

        state = self._fleet_states[v_id]
        state.current_telemetry = telemetry

        # Ensure H3 index is populated
        if not telemetry.h3_index:
            telemetry.h3_index = SpatialIndexer.lat_lng_to_h3(telemetry.latitude, telemetry.longitude)

        # Update sliding window history
        window = self._sliding_windows[v_id]
        window.append(telemetry)

        # 1. Spatial Incident Correlation (O(1) H3 lookup + 1-ring neighborhood)
        nearby_h3_cells = SpatialIndexer.get_k_ring(telemetry.h3_index, k=1)
        encountered_incidents: List[TrafficIncident] = []
        for cell in nearby_h3_cells:
            if cell in self._active_incidents_by_h3:
                encountered_incidents.append(self._active_incidents_by_h3[cell])

        total_delay_impact = sum(inc.delay_impact_minutes for inc in encountered_incidents)

        # 2. Dynamic ETA Projection
        projected_eta = SpatialIndexer.estimate_eta_minutes(
            current_lat=telemetry.latitude,
            current_lon=telemetry.longitude,
            dest_lat=state.destination_lat,
            dest_lon=state.destination_lon,
            current_speed_kmh=telemetry.speed_kmh,
            delay_penalty_minutes=total_delay_impact,
        )
        state.current_eta_minutes = projected_eta

        # 3. Thermodynamic Buffer Calculation
        tbt, safety_margin, is_spoilage_imminent = ThermalDynamicsEngine.evaluate_thermal_risk_margin(
            current_temp_c=telemetry.cargo_temp_c,
            ambient_temp_c=telemetry.ambient_temp_c,
            compressor_status=telemetry.compressor_status,
            cargo_profile=state.cargo_profile,
            projected_eta_minutes=projected_eta,
        )
        state.thermal_buffer_minutes = tbt

        # 4. Multi-Condition Alert Synthesis (CEP Rules)
        alert: Optional[OperationalAlert] = None
        now_utc = telemetry.timestamp if telemetry.timestamp else datetime.now(timezone.utc)

        # Rule 1: Emergency Reroute (Compressor Failure + Spoilage Imminent)
        if (
            telemetry.compressor_status in (CompressorStatus.FAILURE, CompressorStatus.OFF)
            and is_spoilage_imminent
        ):
            alert = OperationalAlert(
                alert_id=f"alert_emg_{v_id}_{int(now_utc.timestamp())}",
                vehicle_id=v_id,
                timestamp=now_utc,
                alert_level=AlertLevel.EMERGENCY_REROUTE,
                current_temp_c=telemetry.cargo_temp_c,
                critical_threshold_c=state.cargo_profile.critical_threshold_c,
                thermal_buffer_minutes=tbt,
                projected_eta_minutes=projected_eta,
                root_cause=f"Refrigeration unit {telemetry.compressor_status.value} with ETA ({projected_eta:.0f}m) exceeding thermal buffer ({tbt:.0f}m)",
                recommended_action="TRIGGER IMMEDIATE REROUTE TO NEAREST COLD-STORAGE CROSS-DOCK FACILITY.",
                h3_cluster=telemetry.h3_index,
                commercial_risk_usd=state.cargo_profile.cargo_value_usd,
            )

        # Rule 2: Critical Disruption (Traffic Incident Causing Spoilage Projection)
        elif is_spoilage_imminent and encountered_incidents:
            primary_inc = encountered_incidents[0]
            alert = OperationalAlert(
                alert_id=f"alert_crit_{v_id}_{int(now_utc.timestamp())}",
                vehicle_id=v_id,
                timestamp=now_utc,
                alert_level=AlertLevel.CRITICAL_DISRUPTION,
                current_temp_c=telemetry.cargo_temp_c,
                critical_threshold_c=state.cargo_profile.critical_threshold_c,
                thermal_buffer_minutes=tbt,
                projected_eta_minutes=projected_eta,
                root_cause=f"Traffic incident '{primary_inc.description}' delayed ETA by {total_delay_impact:.0f}m, destroying thermal buffer.",
                recommended_action="DISPATCH TRAFFIC ESCORT OR ASSIGN DYNAMIC BYPASS ROUTE VIA CORRIDOR.",
                h3_cluster=telemetry.h3_index,
                incident_id=primary_inc.incident_id,
                commercial_risk_usd=state.cargo_profile.cargo_value_usd,
            )

        # Rule 3: Warning (Compressor Degraded OR Narrowing Margin < 20 min)
        elif (
            telemetry.compressor_status == CompressorStatus.DEGRADED
            or (0 <= safety_margin < 20.0 and projected_eta > 20.0)
        ):
            alert = OperationalAlert(
                alert_id=f"alert_warn_{v_id}_{int(now_utc.timestamp())}",
                vehicle_id=v_id,
                timestamp=now_utc,
                alert_level=AlertLevel.WARNING,
                current_temp_c=telemetry.cargo_temp_c,
                critical_threshold_c=state.cargo_profile.critical_threshold_c,
                thermal_buffer_minutes=tbt,
                projected_eta_minutes=projected_eta,
                root_cause=f"Thermal safety margin compressed to {safety_margin:.1f} minutes under ambient {telemetry.ambient_temp_c:.1f}°C.",
                recommended_action="MONITOR TRU TELEMETRY; PREPARE SECONDARY DISPATCH UNIT AT DESTINATION.",
                h3_cluster=telemetry.h3_index,
                commercial_risk_usd=state.cargo_profile.cargo_value_usd * 0.25,
            )

        if alert:
            state.active_alerts.append(alert)
            self._alert_history.append(alert)

        return alert

    def get_fleet_states(self) -> Dict[str, VehicleState]:
        return self._fleet_states

    def get_active_incidents(self) -> List[TrafficIncident]:
        return list(self._active_incidents_by_h3.values())

    def get_alert_history(self) -> List[OperationalAlert]:
        return self._alert_history
