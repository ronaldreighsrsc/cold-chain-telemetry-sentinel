"""
Tests for Dead-Letter Queue (DLQ) and Data Quality validation.
"""

from cold_chain_telemetry_sentinel.engines.dlq_manager import (
    DeadLetterQueueManager,
    DLQReason,
)


def test_dlq_valid_payload():
    dlq = DeadLetterQueueManager()
    valid_payload = {
        "vehicle_id": "TRUCK-01",
        "latitude": -33.45,
        "longitude": -70.65,
        "cargo_temp_c": 3.8,
        "ambient_temp_c": 26.0,
    }
    is_valid, reason, err = dlq.validate_and_clean_telemetry(valid_payload)
    assert is_valid is True
    assert reason is None


def test_dlq_missing_fields_schema_violation():
    dlq = DeadLetterQueueManager()
    bad_payload = {
        "vehicle_id": "TRUCK-01",
        # missing latitude and longitude
        "cargo_temp_c": 3.8,
    }
    is_valid, reason, err = dlq.validate_and_clean_telemetry(bad_payload)
    assert is_valid is False
    assert reason == DLQReason.SCHEMA_VIOLATION


def test_dlq_out_of_bounds_coordinates():
    dlq = DeadLetterQueueManager()
    bad_payload = {
        "vehicle_id": "TRUCK-01",
        "latitude": 95.0,  # Invalid latitude > 90
        "longitude": -70.65,
        "cargo_temp_c": 3.8,
        "ambient_temp_c": 25.0,
    }
    is_valid, reason, err = dlq.validate_and_clean_telemetry(bad_payload)
    assert is_valid is False
    assert reason == DLQReason.OUT_OF_BOUNDS_COORDINATES


def test_dlq_physically_impossible_temperature():
    dlq = DeadLetterQueueManager()
    bad_payload = {
        "vehicle_id": "TRUCK-01",
        "latitude": -33.45,
        "longitude": -70.65,
        "cargo_temp_c": 150.0,  # Sensor malfunction
        "ambient_temp_c": 25.0,
    }
    is_valid, reason, err = dlq.validate_and_clean_telemetry(bad_payload)
    assert is_valid is False
    assert reason == DLQReason.IMPOSSIBLE_TEMPERATURE


def test_dlq_capture_and_stats():
    dlq = DeadLetterQueueManager()
    record = dlq.capture(
        reason=DLQReason.IMPOSSIBLE_TEMPERATURE,
        error_message="Sensor read 150°C",
        raw_payload={"cargo_temp_c": 150.0},
    )
    assert record.reason == DLQReason.IMPOSSIBLE_TEMPERATURE
    stats = dlq.get_stats()
    assert stats["total_rejected_events"] == 1
    assert stats["rejections_by_category"]["IMPOSSIBLE_TEMPERATURE"] == 1
