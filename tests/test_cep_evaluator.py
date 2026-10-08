"""
Tests for Complex Event Processing (CEP) engine and alert dispatch rules.
"""

from datetime import datetime, timezone
from cold_chain_telemetry_sentinel.domain.models import (
    AlertLevel,
    CargoProfile,
    CompressorStatus,
    IncidentSeverity,
    TelemetryRecord,
    TrafficIncident,
)
from cold_chain_telemetry_sentinel.engines.cep_evaluator import ComplexEventEvaluator
from cold_chain_telemetry_sentinel.engines.spatial_indexer import SpatialIndexer


def test_cep_incident_correlation_and_alert():
    cep = ComplexEventEvaluator()
    v_id = "TRUCK-VAX-01"

    # Register vehicle heading to destination
    cep.register_vehicle(
        vehicle_id=v_id,
        cargo_profile=CargoProfile.default_vaccines(),
        destination_lat=-33.40,
        destination_lon=-70.55,
        base_eta_minutes=30.0,
    )

    # Ingest incident near vehicle's location
    incident_lat = -33.4150
    incident_lon = -70.7100
    inc_h3 = SpatialIndexer.lat_lng_to_h3(incident_lat, incident_lon)
    incident = TrafficIncident(
        incident_id="INC-GRIDLOCK-01",
        timestamp=datetime.now(timezone.utc),
        latitude=incident_lat,
        longitude=incident_lon,
        severity=IncidentSeverity.CRITICAL_GRIDLOCK,
        delay_impact_minutes=60.0,
        description="Massive highway blockage",
        h3_index=inc_h3,
    )
    cep.ingest_incident(incident)

    # Ingest telemetry ping at same H3 index with failing compressor
    telemetry = TelemetryRecord(
        vehicle_id=v_id,
        timestamp=datetime.now(timezone.utc),
        latitude=incident_lat,
        longitude=incident_lon,
        speed_kmh=10.0,
        ambient_temp_c=34.0,
        cargo_temp_c=5.5,
        compressor_status=CompressorStatus.FAILURE,
        battery_soc_pct=85.0,
        h3_index=inc_h3,
    )

    alert = cep.process_telemetry(telemetry)
    assert alert is not None
    # Must trigger EMERGENCY_REROUTE because compressor failed and delay destroys buffer
    assert alert.alert_level in (AlertLevel.EMERGENCY_REROUTE, AlertLevel.CRITICAL_DISRUPTION)
    assert alert.vehicle_id == v_id
    assert alert.commercial_risk_usd > 50000


def test_cep_normal_telemetry_no_alert():
    cep = ComplexEventEvaluator()
    v_id = "TRUCK-VAX-HEALTHY"

    cep.register_vehicle(
        vehicle_id=v_id,
        cargo_profile=CargoProfile.default_vaccines(),
        destination_lat=-33.40,
        destination_lon=-70.55,
    )

    telemetry = TelemetryRecord(
        vehicle_id=v_id,
        timestamp=datetime.now(timezone.utc),
        latitude=-33.40,
        longitude=-70.75,
        speed_kmh=75.0,
        ambient_temp_c=22.0,
        cargo_temp_c=4.0,
        compressor_status=CompressorStatus.OPTIMAL,
        battery_soc_pct=98.0,
    )

    alert = cep.process_telemetry(telemetry)
    # Healthy truck with optimal refrigeration in clear traffic requires no alert
    assert alert is None
