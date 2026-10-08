"""
Tests for domain models and data contracts.
"""

from datetime import datetime, timezone
from cold_chain_telemetry_sentinel.domain.models import (
    CargoProfile,
    CargoType,
    CompressorStatus,
    TelemetryRecord,
    TrafficIncident,
    IncidentSeverity,
    OperationalAlert,
    AlertLevel,
)


def test_cargo_profiles_defaults():
    vax = CargoProfile.default_vaccines()
    assert vax.cargo_type == CargoType.VACCINES_PHARMA
    assert vax.min_temp_c == 2.0
    assert vax.max_temp_c == 8.0
    assert vax.critical_threshold_c == 8.5
    assert vax.cargo_value_usd > 50000

    frozen = CargoProfile.default_frozen()
    assert frozen.cargo_type == CargoType.FROZEN_SEAFOOD
    assert frozen.critical_threshold_c == -15.0

    dairy = CargoProfile.default_dairy()
    assert dairy.cargo_type == CargoType.FRESH_DAIRY_PRODUCE
    assert dairy.critical_threshold_c == 6.0


def test_telemetry_record_serialization():
    now = datetime.now(timezone.utc)
    rec = TelemetryRecord(
        vehicle_id="TRUCK-TEST-01",
        timestamp=now,
        latitude=-33.45,
        longitude=-70.65,
        speed_kmh=60.0,
        ambient_temp_c=25.0,
        cargo_temp_c=4.0,
        compressor_status=CompressorStatus.OPTIMAL,
        battery_soc_pct=95.0,
        h3_index="88b2c55733fffff",
    )
    d = rec.to_dict()
    assert d["vehicle_id"] == "TRUCK-TEST-01"
    assert d["compressor_status"] == "OPTIMAL"
    assert d["cargo_temp_c"] == 4.0
    assert d["is_valid"] is True


def test_traffic_incident_serialization():
    now = datetime.now(timezone.utc)
    inc = TrafficIncident(
        incident_id="INC-01",
        timestamp=now,
        latitude=-33.41,
        longitude=-70.70,
        severity=IncidentSeverity.CRITICAL_GRIDLOCK,
        delay_impact_minutes=45.0,
        description="Major highway gridlock",
    )
    d = inc.to_dict()
    assert d["incident_id"] == "INC-01"
    assert d["severity"] == "CRITICAL_GRIDLOCK"
    assert d["delay_impact_minutes"] == 45.0


def test_operational_alert_serialization():
    now = datetime.now(timezone.utc)
    alert = OperationalAlert(
        alert_id="ALT-01",
        vehicle_id="TRUCK-01",
        timestamp=now,
        alert_level=AlertLevel.CRITICAL_DISRUPTION,
        current_temp_c=5.5,
        critical_threshold_c=8.5,
        thermal_buffer_minutes=25.0,
        projected_eta_minutes=40.0,
        root_cause="Severe delay",
        recommended_action="Reroute",
        commercial_risk_usd=120000.0,
    )
    d = alert.to_dict()
    assert d["alert_id"] == "ALT-01"
    assert d["alert_level"] == "CRITICAL_DISRUPTION"
    assert d["commercial_risk_usd"] == 120000.0
