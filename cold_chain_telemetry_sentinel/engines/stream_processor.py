"""
Stream Processor engine.
Orchestrates end-to-end streaming ingestion, dead-letter routing,
complex event processing (CEP), state persistence, and alert emissions.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from cold_chain_telemetry_sentinel.connectors.analytics_sink import AnalyticsSink
from cold_chain_telemetry_sentinel.connectors.broker import IEventBroker, InMemoryEventBroker
from cold_chain_telemetry_sentinel.connectors.state_store import IStateStore, InMemoryStateStore
from cold_chain_telemetry_sentinel.connectors.webhook_emitter import WebhookEmitter
from cold_chain_telemetry_sentinel.domain.models import (
    CompressorStatus,
    IncidentSeverity,
    OperationalAlert,
    TelemetryRecord,
    TrafficIncident,
)
from cold_chain_telemetry_sentinel.engines.cep_evaluator import ComplexEventEvaluator
from cold_chain_telemetry_sentinel.engines.dlq_manager import DeadLetterQueueManager, DLQReason

logger = logging.getLogger(__name__)


class StreamProcessor:
    """
    Central real-time stream execution pipeline.
    """

    TOPIC_RAW_TELEMETRY = "coldchain.telemetry.raw"
    TOPIC_TRAFFIC_INCIDENTS = "coldchain.traffic.incidents"
    TOPIC_ALERTS = "coldchain.alerts.dispatched"
    TOPIC_DLQ = "coldchain.telemetry.dlq"

    def __init__(
        self,
        broker: Optional[IEventBroker] = None,
        state_store: Optional[IStateStore] = None,
        analytics_sink: Optional[AnalyticsSink] = None,
        webhook_emitter: Optional[WebhookEmitter] = None,
    ):
        self.broker = broker or InMemoryEventBroker()
        self.state_store = state_store or InMemoryStateStore()
        self.analytics_sink = analytics_sink or AnalyticsSink()
        self.webhook_emitter = webhook_emitter or WebhookEmitter()

        self.dlq_manager = DeadLetterQueueManager()
        self.cep_evaluator = ComplexEventEvaluator()

        # Telemetry metrics
        self.metrics = {
            "consumed_events": 0,
            "processed_valid_events": 0,
            "dlq_rejected_events": 0,
            "incidents_ingested": 0,
            "alerts_dispatched": 0,
            "avg_latency_ms": 0.0,
        }

        self._setup_subscriptions()

    def _setup_subscriptions(self) -> None:
        """Wires message topics to processor callbacks."""
        self.broker.subscribe(self.TOPIC_RAW_TELEMETRY, self.handle_telemetry_event)
        self.broker.subscribe(self.TOPIC_TRAFFIC_INCIDENTS, self.handle_incident_event)

    def handle_telemetry_event(self, raw_payload: Dict[str, Any]) -> Optional[OperationalAlert]:
        """
        Stream handler for incoming telemetry records.
        Validates payload, routes to DLQ if invalid, executes CEP rules,
        and saves state in sub-millisecond execution time.
        """
        start_time = time.perf_counter()
        self.metrics["consumed_events"] += 1

        # 1. Validation & DLQ Gate
        is_valid, dlq_reason, err_msg = self.dlq_manager.validate_and_clean_telemetry(raw_payload)
        if not is_valid:
            self.metrics["dlq_rejected_events"] += 1
            record = self.dlq_manager.capture(
                reason=dlq_reason,
                error_message=err_msg or "Validation error",
                raw_payload=raw_payload,
                source_topic=self.TOPIC_RAW_TELEMETRY,
            )
            self.broker.publish(self.TOPIC_DLQ, {
                "record_id": record.record_id,
                "reason": record.reason.value,
                "error": record.error_message,
                "payload": raw_payload,
            })
            return None

        # 2. Domain Object Deserialization
        try:
            ts_val = raw_payload.get("timestamp")
            if isinstance(ts_val, str):
                ts = datetime.fromisoformat(ts_val)
            elif isinstance(ts_val, datetime):
                ts = ts_val
            else:
                ts = datetime.now(timezone.utc)

            compressor_raw = raw_payload.get("compressor_status", "OPTIMAL")
            try:
                comp_status = CompressorStatus(compressor_raw)
            except ValueError:
                comp_status = CompressorStatus.OPTIMAL

            telemetry = TelemetryRecord(
                vehicle_id=str(raw_payload["vehicle_id"]),
                timestamp=ts,
                latitude=float(raw_payload["latitude"]),
                longitude=float(raw_payload["longitude"]),
                speed_kmh=float(raw_payload.get("speed_kmh", 50.0)),
                ambient_temp_c=float(raw_payload.get("ambient_temp_c", 25.0)),
                cargo_temp_c=float(raw_payload.get("cargo_temp_c", 4.0)),
                compressor_status=comp_status,
                battery_soc_pct=float(raw_payload.get("battery_soc_pct", 95.0)),
                h3_index=str(raw_payload.get("h3_index", "")),
            )
        except Exception as e:
            self.metrics["dlq_rejected_events"] += 1
            self.dlq_manager.capture(
                reason=DLQReason.DESERIALIZATION_FAILURE,
                error_message=str(e),
                raw_payload=raw_payload,
            )
            return None

        # 3. CEP Evaluation & Threat Detection
        alert = self.cep_evaluator.process_telemetry(telemetry)

        # 4. State Store Update
        self.state_store.set_vehicle_telemetry(telemetry.vehicle_id, telemetry.to_dict())
        if telemetry.h3_index:
            self.state_store.add_to_spatial_cluster(telemetry.h3_index, telemetry.vehicle_id)

        # 5. Analytical OLAP Sink
        self.analytics_sink.insert_telemetry_batch([telemetry.to_dict()])

        # 6. Alert Dispatching & Webhook Execution
        if alert:
            self.metrics["alerts_dispatched"] += 1
            alert_dict = alert.to_dict()
            self.state_store.save_alert(alert_dict)
            self.analytics_sink.insert_alert(alert_dict)
            self.broker.publish(self.TOPIC_ALERTS, alert_dict)
            self.webhook_emitter.dispatch_alert(alert_dict)

        # Update metrics
        self.metrics["processed_valid_events"] += 1
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        prev_avg = self.metrics["avg_latency_ms"]
        n = self.metrics["processed_valid_events"]
        self.metrics["avg_latency_ms"] = round(prev_avg + (elapsed_ms - prev_avg) / n, 3)

        return alert

    def handle_incident_event(self, raw_incident: Dict[str, Any]) -> None:
        """
        Stream handler for traffic incident notifications.
        """
        self.metrics["incidents_ingested"] += 1
        try:
            ts_val = raw_incident.get("timestamp")
            if isinstance(ts_val, str):
                ts = datetime.fromisoformat(ts_val)
            elif isinstance(ts_val, datetime):
                ts = ts_val
            else:
                ts = datetime.now(timezone.utc)

            sev_raw = raw_incident.get("severity", "MEDIUM")
            try:
                severity = IncidentSeverity(sev_raw)
            except ValueError:
                severity = IncidentSeverity.MEDIUM

            incident = TrafficIncident(
                incident_id=str(raw_incident["incident_id"]),
                timestamp=ts,
                latitude=float(raw_incident["latitude"]),
                longitude=float(raw_incident["longitude"]),
                severity=severity,
                delay_impact_minutes=float(raw_incident.get("delay_impact_minutes", 20.0)),
                description=str(raw_incident.get("description", "Road incident")),
                h3_index=str(raw_incident.get("h3_index", "")),
                active=bool(raw_incident.get("active", True)),
            )
            self.cep_evaluator.ingest_incident(incident)
        except Exception as e:
            logger.error(f"Error handling incident event: {e}")
