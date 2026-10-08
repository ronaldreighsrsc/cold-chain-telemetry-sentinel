"""
REST API Microservice for Cold-Chain Telemetry Sentinel.
Provides endpoints for high-throughput HTTP telemetry ingestion,
spatial incident registration, and real-time fleet queries.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from cold_chain_telemetry_sentinel.engines.stream_processor import StreamProcessor

app = FastAPI(
    title="Cold-Chain Telemetry Sentinel API",
    description="Real-Time Geospatial IoT Telemetry & Spoilage Prevention Microservice",
    version="1.0.0",
)

# Global singleton pipeline instance for the API process
processor = StreamProcessor()


class TelemetryIngestRequest(BaseModel):
    vehicle_id: str = Field(..., examples=["TRUCK-VAX-01"])
    latitude: float = Field(..., ge=-90.0, le=90.0, examples=[-33.4150])
    longitude: float = Field(..., ge=-180.0, le=180.0, examples=[-70.7100])
    cargo_temp_c: float = Field(..., examples=[4.2])
    ambient_temp_c: float = Field(default=28.0, examples=[32.0])
    speed_kmh: float = Field(default=55.0, ge=0.0, examples=[45.0])
    compressor_status: str = Field(default="OPTIMAL", examples=["OPTIMAL"])
    battery_soc_pct: float = Field(default=95.0, ge=0.0, le=100.0, examples=[92.5])


class TrafficIncidentRequest(BaseModel):
    incident_id: str = Field(..., examples=["INC-CHOKEPOINT-01"])
    latitude: float = Field(..., ge=-90.0, le=90.0, examples=[-33.4150])
    longitude: float = Field(..., ge=-180.0, le=180.0, examples=[-70.7100])
    severity: str = Field(default="HIGH", examples=["CRITICAL_GRIDLOCK"])
    delay_impact_minutes: float = Field(default=35.0, ge=0.0, examples=[45.0])
    description: str = Field(..., examples=["Major highway gridlock due to overturned tanker"])


@app.get("/health", status_code=status.HTTP_200_OK)
def health_check() -> Dict[str, str]:
    """Liveness probe for container orchestrators (Kubernetes / Docker)."""
    return {
        "status": "healthy",
        "service": "cold-chain-telemetry-sentinel",
        "engine": "event-driven-cep",
    }


@app.post("/api/v1/telemetry", status_code=status.HTTP_202_ACCEPTED)
def ingest_telemetry(payload: TelemetryIngestRequest) -> Dict[str, Any]:
    """
    Ingests an IoT telemetry ping.
    Evaluates schema bounds, updates state, and generates alerts if thermal buffer is threatened.
    """
    data = payload.model_dump()
    alert = processor.handle_telemetry_event(data)
    return {
        "status": "accepted",
        "vehicle_id": payload.vehicle_id,
        "alert_triggered": alert is not None,
        "alert_level": alert.alert_level.value if alert else None,
    }


@app.post("/api/v1/incidents", status_code=status.HTTP_201_CREATED)
def register_incident(payload: TrafficIncidentRequest) -> Dict[str, Any]:
    """
    Registers a dynamic traffic incident and updates the spatial disruption index.
    """
    data = payload.model_dump()
    processor.handle_incident_event(data)
    return {
        "status": "registered",
        "incident_id": payload.incident_id,
        "h3_index": processor.cep_evaluator.get_active_incidents()[-1].h3_index if processor.cep_evaluator.get_active_incidents() else "",
    }


@app.get("/api/v1/fleet")
def get_fleet_state() -> List[Dict[str, Any]]:
    """Returns active real-time positions and sensor readings for all tracked vehicles."""
    return processor.state_store.get_all_vehicles()


@app.get("/api/v1/alerts")
def get_active_alerts(limit: int = 50) -> List[Dict[str, Any]]:
    """Returns chronologically dispatched operational alerts."""
    return processor.state_store.get_active_alerts(limit=limit)


@app.get("/api/v1/analytics/kpis")
def get_analytics_kpis() -> Dict[str, Any]:
    """Executes embedded DuckDB OLAP queries over historical stream events."""
    return processor.analytics_sink.get_fleet_summary()


@app.get("/api/v1/metrics")
def get_pipeline_metrics() -> Dict[str, Any]:
    """Exposes real-time stream processor throughput, latency, and DLQ statistics."""
    return {
        "metrics": processor.metrics,
        "dlq": processor.dlq_manager.get_stats(),
    }
