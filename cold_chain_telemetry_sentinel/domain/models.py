"""
Domain models for Cold-Chain Telemetry Sentinel.
Adheres to Clean Architecture and strict type safety.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class CargoType(str, Enum):
    """Classifies temperature-controlled pharmaceutical and perishable shipments."""
    VACCINES_PHARMA = "VACCINES_PHARMA"
    FROZEN_SEAFOOD = "FROZEN_SEAFOOD"
    FRESH_DAIRY_PRODUCE = "FRESH_DAIRY_PRODUCE"
    BIOLOGICAL_SAMPLES = "BIOLOGICAL_SAMPLES"


@dataclass(frozen=True)
class CargoProfile:
    """Thermal tolerance and commercial value envelope for a monitored shipment."""
    cargo_type: CargoType
    min_temp_c: float
    max_temp_c: float
    critical_threshold_c: float  # Absolute boundary before irreversible spoilage
    thermal_inertia_k: float     # Newton cooling decay coefficient (lower = better insulation)
    cargo_value_usd: float       # Commercial exposure if shipment is spoiled
    sla_max_transit_minutes: int

    @classmethod
    def default_vaccines(cls) -> CargoProfile:
        return cls(
            cargo_type=CargoType.VACCINES_PHARMA,
            min_temp_c=2.0,
            max_temp_c=8.0,
            critical_threshold_c=8.5,
            thermal_inertia_k=0.012,  # Insulated thermal container
            cargo_value_usd=120_000.0,
            sla_max_transit_minutes=180
        )

    @classmethod
    def default_frozen(cls) -> CargoProfile:
        return cls(
            cargo_type=CargoType.FROZEN_SEAFOOD,
            min_temp_c=-25.0,
            max_temp_c=-18.0,
            critical_threshold_c=-15.0,
            thermal_inertia_k=0.018,
            cargo_value_usd=45_000.0,
            sla_max_transit_minutes=240
        )

    @classmethod
    def default_dairy(cls) -> CargoProfile:
        return cls(
            cargo_type=CargoType.FRESH_DAIRY_PRODUCE,
            min_temp_c=1.0,
            max_temp_c=4.0,
            critical_threshold_c=6.0,
            thermal_inertia_k=0.025,
            cargo_value_usd=28_000.0,
            sla_max_transit_minutes=150
        )


class CompressorStatus(str, Enum):
    """Operational health of active transport refrigeration unit (TRU)."""
    OPTIMAL = "OPTIMAL"
    DEGRADED = "DEGRADED"      # 50% cooling capacity
    FAILURE = "FAILURE"        # 0% cooling, passive decay only
    OFF = "OFF"


class IncidentSeverity(str, Enum):
    """Traffic and road disruption severity scale."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL_GRIDLOCK = "CRITICAL_GRIDLOCK"


class AlertLevel(str, Enum):
    """Escalation tier for operational dispatch actions."""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL_DISRUPTION = "CRITICAL_DISRUPTION"
    EMERGENCY_REROUTE = "EMERGENCY_REROUTE"


@dataclass
class TelemetryRecord:
    """Single high-frequency GPS + environmental probe ping from refrigerated truck."""
    vehicle_id: str
    timestamp: datetime
    latitude: float
    longitude: float
    speed_kmh: float
    ambient_temp_c: float
    cargo_temp_c: float
    compressor_status: CompressorStatus
    battery_soc_pct: float
    h3_index: str = ""
    is_valid: bool = True
    validation_error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": self.timestamp.isoformat(),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "speed_kmh": self.speed_kmh,
            "ambient_temp_c": self.ambient_temp_c,
            "cargo_temp_c": self.cargo_temp_c,
            "compressor_status": self.compressor_status.value,
            "battery_soc_pct": self.battery_soc_pct,
            "h3_index": self.h3_index,
            "is_valid": self.is_valid,
            "validation_error": self.validation_error,
        }


@dataclass
class TrafficIncident:
    """Geospatial traffic disruption ingested from real-time feeds."""
    incident_id: str
    timestamp: datetime
    latitude: float
    longitude: float
    severity: IncidentSeverity
    delay_impact_minutes: float
    description: str
    h3_index: str = ""
    active: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "timestamp": self.timestamp.isoformat(),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "severity": self.severity.value,
            "delay_impact_minutes": self.delay_impact_minutes,
            "description": self.description,
            "h3_index": self.h3_index,
            "active": self.active,
        }


@dataclass
class OperationalAlert:
    """Actionable alert dispatched to cold-chain control tower."""
    alert_id: str
    vehicle_id: str
    timestamp: datetime
    alert_level: AlertLevel
    current_temp_c: float
    critical_threshold_c: float
    thermal_buffer_minutes: float
    projected_eta_minutes: float
    root_cause: str
    recommended_action: str
    h3_cluster: str = ""
    incident_id: Optional[str] = None
    commercial_risk_usd: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "vehicle_id": self.vehicle_id,
            "timestamp": self.timestamp.isoformat(),
            "alert_level": self.alert_level.value,
            "current_temp_c": self.current_temp_c,
            "critical_threshold_c": self.critical_threshold_c,
            "thermal_buffer_minutes": round(self.thermal_buffer_minutes, 1),
            "projected_eta_minutes": round(self.projected_eta_minutes, 1),
            "root_cause": self.root_cause,
            "recommended_action": self.recommended_action,
            "h3_cluster": self.h3_cluster,
            "incident_id": self.incident_id,
            "commercial_risk_usd": self.commercial_risk_usd,
        }


@dataclass
class VehicleState:
    """Aggregated real-time state maintained in memory/Redis for active fleet assets."""
    vehicle_id: str
    cargo_profile: CargoProfile
    current_telemetry: Optional[TelemetryRecord] = None
    destination_lat: float = 0.0
    destination_lon: float = 0.0
    base_eta_minutes: float = 60.0
    current_eta_minutes: float = 60.0
    thermal_buffer_minutes: float = 120.0
    active_alerts: list[OperationalAlert] = field(default_factory=list)
    recent_h3_indices: list[str] = field(default_factory=list)
