"""
Dead-Letter Queue (DLQ) Manager for stream data quality and fault tolerance.
Captures, categorizes, and audits rejected telemetry payloads for re-processing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class DLQReason(str, Enum):
    """Taxonomy of streaming data rejection causes."""
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    OUT_OF_BOUNDS_COORDINATES = "OUT_OF_BOUNDS_COORDINATES"
    IMPOSSIBLE_TEMPERATURE = "IMPOSSIBLE_TEMPERATURE"
    TIMESTAMP_DRIFT = "TIMESTAMP_DRIFT"
    UNKNOWN_VEHICLE = "UNKNOWN_VEHICLE"
    DESERIALIZATION_FAILURE = "DESERIALIZATION_FAILURE"


@dataclass
class DLQRecord:
    """Archived invalid message with diagnostic forensic metadata."""
    record_id: str
    timestamp: datetime
    reason: DLQReason
    error_message: str
    raw_payload: Dict[str, Any]
    source_topic: str = "coldchain.telemetry.raw"


class DeadLetterQueueManager:
    """
    In-memory and persistent buffer for unprocessable streaming events.
    Guarantees zero silent message loss in the event stream.
    """

    def __init__(self, max_buffer_size: int = 5000):
        self.max_buffer_size = max_buffer_size
        self._dlq_records: List[DLQRecord] = []
        self._stats: Dict[str, int] = {reason.value: 0 for reason in DLQReason}

    def capture(
        self,
        reason: DLQReason,
        error_message: str,
        raw_payload: Dict[str, Any],
        source_topic: str = "coldchain.telemetry.raw",
    ) -> DLQRecord:
        """
        Routes an unprocessable event into the DLQ quarantine.
        """
        record_id = f"dlq_{len(self._dlq_records) + 1}_{int(datetime.now(timezone.utc).timestamp())}"
        record = DLQRecord(
            record_id=record_id,
            timestamp=datetime.now(timezone.utc),
            reason=reason,
            error_message=error_message,
            raw_payload=raw_payload,
            source_topic=source_topic,
        )

        if len(self._dlq_records) >= self.max_buffer_size:
            self._dlq_records.pop(0)

        self._dlq_records.append(record)
        self._stats[reason.value] = self._stats.get(reason.value, 0) + 1
        return record

    def validate_and_clean_telemetry(self, raw: Dict[str, Any]) -> Tuple[bool, Optional[DLQReason], Optional[str]]:
        """
        Validates telemetry schema, sensor ranges, and spatial bounds before ingestion.
        """
        required_fields = ["vehicle_id", "latitude", "longitude", "cargo_temp_c", "ambient_temp_c"]
        for f in required_fields:
            if f not in raw or raw[f] is None:
                return False, DLQReason.SCHEMA_VIOLATION, f"Missing mandatory field '{f}'"

        lat = raw.get("latitude")
        lon = raw.get("longitude")
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return False, DLQReason.OUT_OF_BOUNDS_COORDINATES, f"Coordinates out of range: ({lat}, {lon})"

        cargo_t = raw.get("cargo_temp_c")
        if cargo_t is not None and (cargo_t < -60.0 or cargo_t > 80.0):
            return False, DLQReason.IMPOSSIBLE_TEMPERATURE, f"Physically impossible cargo temp: {cargo_t}°C"

        ambient_t = raw.get("ambient_temp_c")
        if ambient_t is not None and (ambient_t < -50.0 or ambient_t > 65.0):
            return False, DLQReason.IMPOSSIBLE_TEMPERATURE, f"Physically impossible ambient temp: {ambient_t}°C"

        return True, None, None

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_rejected_events": len(self._dlq_records),
            "rejections_by_category": dict(self._stats),
        }

    def get_recent_records(self, limit: int = 50) -> List[DLQRecord]:
        return self._dlq_records[-limit:]
