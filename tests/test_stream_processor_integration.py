"""
Integration tests for Stream Processor, In-Memory Broker, State Store, DuckDB sink, and Webhooks.
"""

from datetime import datetime, timezone
from cold_chain_telemetry_sentinel.connectors.analytics_sink import AnalyticsSink
from cold_chain_telemetry_sentinel.connectors.broker import InMemoryEventBroker
from cold_chain_telemetry_sentinel.connectors.state_store import InMemoryStateStore
from cold_chain_telemetry_sentinel.connectors.webhook_emitter import WebhookEmitter
from cold_chain_telemetry_sentinel.domain.models import CompressorStatus
from cold_chain_telemetry_sentinel.engines.stream_processor import StreamProcessor
from cold_chain_telemetry_sentinel.services.simulator import FleetSimulator


def test_stream_processor_end_to_end_flow():
    broker = InMemoryEventBroker()
    state_store = InMemoryStateStore()
    analytics_sink = AnalyticsSink(":memory:")
    webhook_emitter = WebhookEmitter()

    processor = StreamProcessor(
        broker=broker,
        state_store=state_store,
        analytics_sink=analytics_sink,
        webhook_emitter=webhook_emitter,
    )

    # 1. Ingest disruption
    disruption = {
        "incident_id": "INC-TEST-01",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": -33.4150,
        "longitude": -70.7100,
        "severity": "CRITICAL_GRIDLOCK",
        "delay_impact_minutes": 50.0,
        "description": "Bridge collapse blocking corridor",
    }
    processor.broker.publish(StreamProcessor.TOPIC_TRAFFIC_INCIDENTS, disruption)
    assert processor.metrics["incidents_ingested"] == 1

    # 2. Ingest valid telemetry ping
    valid_event = {
        "vehicle_id": "TRUCK-TEST-01",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": -33.4150,
        "longitude": -70.7100,
        "speed_kmh": 12.0,
        "ambient_temp_c": 32.0,
        "cargo_temp_c": 5.8,
        "compressor_status": CompressorStatus.FAILURE.value,
        "battery_soc_pct": 80.0,
    }
    alert = processor.handle_telemetry_event(valid_event)
    assert processor.metrics["processed_valid_events"] == 1
    assert alert is not None

    # Verify state store was updated
    stored_telemetry = state_store.get_vehicle_telemetry("TRUCK-TEST-01")
    assert stored_telemetry is not None
    assert stored_telemetry["vehicle_id"] == "TRUCK-TEST-01"

    # Verify DuckDB OLAP captured event
    summary = analytics_sink.get_fleet_summary()
    assert summary["total_events"] == 1
    assert summary["active_vehicles"] == 1

    # Verify webhook was dispatched
    deliveries = webhook_emitter.get_delivery_history()
    assert len(deliveries) >= 1
    assert deliveries[0].success is True


def test_stream_processor_dlq_rejection():
    processor = StreamProcessor()
    corrupt_event = {
        "vehicle_id": "TRUCK-MALFORMED",
        # Missing coordinates
        "cargo_temp_c": 4.0,
    }
    alert = processor.handle_telemetry_event(corrupt_event)
    assert alert is None
    assert processor.metrics["dlq_rejected_events"] == 1
    assert processor.metrics["processed_valid_events"] == 0
