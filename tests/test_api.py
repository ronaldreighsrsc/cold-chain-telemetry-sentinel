"""
Tests for FastAPI REST microservice endpoints.
"""

from fastapi.testclient import TestClient
from cold_chain_telemetry_sentinel.services.api import app

client = TestClient(app)


def test_health_check_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "cold-chain-telemetry-sentinel"


def test_ingest_telemetry_endpoint():
    payload = {
        "vehicle_id": "TRUCK-API-01",
        "latitude": -33.4150,
        "longitude": -70.7100,
        "cargo_temp_c": 3.5,
        "ambient_temp_c": 28.0,
        "speed_kmh": 60.0,
        "compressor_status": "OPTIMAL",
        "battery_soc_pct": 94.0,
    }
    response = client.post("/api/v1/telemetry", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["vehicle_id"] == "TRUCK-API-01"


def test_register_incident_endpoint():
    incident_payload = {
        "incident_id": "INC-API-01",
        "latitude": -33.4150,
        "longitude": -70.7100,
        "severity": "CRITICAL_GRIDLOCK",
        "delay_impact_minutes": 40.0,
        "description": "API test road disruption",
    }
    response = client.post("/api/v1/incidents", json=incident_payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "registered"
    assert data["incident_id"] == "INC-API-01"


def test_get_fleet_state_endpoint():
    response = client.get("/api/v1/fleet")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_metrics_endpoint():
    response = client.get("/api/v1/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "metrics" in data
    assert "dlq" in data
