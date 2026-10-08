"""
Tests for thermodynamic physics engine and Thermal Buffer Time forecasting.
"""

import math
from cold_chain_telemetry_sentinel.domain.models import (
    CargoProfile,
    CompressorStatus,
)
from cold_chain_telemetry_sentinel.domain.thermal_model import ThermalDynamicsEngine


def test_thermal_buffer_optimal_compressor():
    profile = CargoProfile.default_vaccines()
    # At normal ambient (28°C) and optimal compressor, cooling capacity exceeds heat infiltration
    tbt = ThermalDynamicsEngine.calculate_thermal_buffer_time(
        current_temp_c=4.0,
        ambient_temp_c=28.0,
        compressor_status=CompressorStatus.OPTIMAL,
        cargo_profile=profile,
    )
    assert math.isinf(tbt) or tbt > 500


def test_thermal_buffer_compressor_failure():
    profile = CargoProfile.default_vaccines()
    # If compressor has failed and ambient is 32°C, TBT must be a finite positive number of minutes
    tbt = ThermalDynamicsEngine.calculate_thermal_buffer_time(
        current_temp_c=5.0,
        ambient_temp_c=32.0,
        compressor_status=CompressorStatus.FAILURE,
        cargo_profile=profile,
    )
    assert 0.0 < tbt < 150.0  # Typically around 15-40 minutes before hitting 8.5°C critical threshold


def test_thermal_buffer_breached():
    profile = CargoProfile.default_vaccines()
    # Already above critical threshold (8.5°C)
    tbt = ThermalDynamicsEngine.calculate_thermal_buffer_time(
        current_temp_c=9.0,
        ambient_temp_c=30.0,
        compressor_status=CompressorStatus.FAILURE,
        cargo_profile=profile,
    )
    assert tbt == 0.0


def test_projected_temperature_cooling_down():
    profile = CargoProfile.default_vaccines()
    # Optimal compressor cools down warm compartment towards setpoint
    future_t = ThermalDynamicsEngine.project_future_temperature(
        current_temp_c=6.5,
        ambient_temp_c=25.0,
        compressor_status=CompressorStatus.OPTIMAL,
        cargo_profile=profile,
        elapsed_minutes=10.0,
    )
    assert future_t < 6.5


def test_thermal_risk_margin_spoilage_imminent():
    profile = CargoProfile.default_vaccines()
    # If TBT is 25 minutes and projected ETA is 45 minutes -> Spoilage Imminent
    tbt, safety_margin, is_spoilage = ThermalDynamicsEngine.evaluate_thermal_risk_margin(
        current_temp_c=6.0,
        ambient_temp_c=35.0,
        compressor_status=CompressorStatus.FAILURE,
        cargo_profile=profile,
        projected_eta_minutes=45.0,
    )
    assert is_spoilage is True
    assert safety_margin < 0.0
