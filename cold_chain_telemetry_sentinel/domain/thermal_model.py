"""
Thermal physics engine and degradation dynamics for refrigerated cargo.
Implements Newton's Law of Cooling, Active Refrigeration balancing,
and Thermal Buffer Time (TBT) forecasting.
"""

from __future__ import annotations

import math
from typing import Tuple
from cold_chain_telemetry_sentinel.domain.models import (
    CargoProfile,
    CompressorStatus,
    TelemetryRecord,
)


class ThermalDynamicsEngine:
    """
    Computes thermodynamic evolution of refrigerated compartments under
    combined ambient heat infiltration and compressor mechanical output.
    """

    @staticmethod
    def calculate_thermal_buffer_time(
        current_temp_c: float,
        ambient_temp_c: float,
        compressor_status: CompressorStatus,
        cargo_profile: CargoProfile,
    ) -> float:
        """
        Calculates Thermal Buffer Time (TBT) in minutes:
        The remaining operational window before cargo exceeds critical spoilage threshold.

        Returns:
            float: Minutes remaining. Returns float('inf') if refrigeration is holding steady.
        """
        critical_t = cargo_profile.critical_threshold_c

        # Case 1: Cargo has already breached critical boundary
        if current_temp_c >= critical_t:
            return 0.0

        # Case 2: Ambient is cooler or equal to cargo (no heat infiltration risk)
        if ambient_temp_c <= critical_t:
            return float("inf")

        k = cargo_profile.thermal_inertia_k

        # Case 3: Compressor health adjustment
        if compressor_status == CompressorStatus.OPTIMAL:
            # Active TRU unit is capable of removing heat faster than infiltration
            # Unless ambient is catastrophic (>45°C) and delta is extreme
            effective_cooling_power = 0.35  # °C removed per minute
            passive_heat_gain = k * (ambient_temp_c - current_temp_c)
            if effective_cooling_power > passive_heat_gain:
                return float("inf")  # Thermal equilibrium maintained
            # Otherwise Net warming rate:
            net_rate = passive_heat_gain - effective_cooling_power
            minutes = (critical_t - current_temp_c) / max(net_rate, 0.001)
            return max(0.0, minutes)

        elif compressor_status == CompressorStatus.DEGRADED:
            # 50% capacity
            effective_cooling_power = 0.12
            passive_heat_gain = k * (ambient_temp_c - current_temp_c)
            net_rate = passive_heat_gain - effective_cooling_power
            if net_rate <= 0:
                return float("inf")
            minutes = (critical_t - current_temp_c) / max(net_rate, 0.001)
            return max(0.0, minutes)

        else:
            # FAILURE or OFF: Pure Newton Cooling decay
            # T(t) = T_amb + (T_0 - T_amb) * exp(-k * t)
            # (T_crit - T_amb) / (T_0 - T_amb) = exp(-k * t)
            ratio = (critical_t - ambient_temp_c) / (current_temp_c - ambient_temp_c)
            if ratio <= 0 or ratio >= 1:
                return 0.0
            t_minutes = - (1.0 / k) * math.log(ratio)
            return max(0.0, t_minutes)

    @staticmethod
    def project_future_temperature(
        current_temp_c: float,
        ambient_temp_c: float,
        compressor_status: CompressorStatus,
        cargo_profile: CargoProfile,
        elapsed_minutes: float,
    ) -> float:
        """
        Projects cargo temperature after elapsed_minutes under current conditions.
        """
        if elapsed_minutes <= 0:
            return current_temp_c

        k = cargo_profile.thermal_inertia_k

        if compressor_status == CompressorStatus.OPTIMAL:
            # Target middle of safe range
            target_t = (cargo_profile.min_temp_c + cargo_profile.max_temp_c) / 2.0
            if current_temp_c > target_t:
                # Cooling down
                cooling_rate = 0.15
                new_t = max(target_t, current_temp_c - cooling_rate * elapsed_minutes)
            else:
                new_t = current_temp_c
            return new_t

        elif compressor_status == CompressorStatus.DEGRADED:
            # Slower cooling or mild warming
            passive_gain = k * (ambient_temp_c - current_temp_c)
            net_warming = (passive_gain - 0.10) * elapsed_minutes
            return current_temp_c + net_warming

        else:
            # Failure / Off: Pure Newton cooling towards ambient
            # T(t) = T_amb + (T_0 - T_amb) * exp(-k * t)
            decay = math.exp(-k * elapsed_minutes)
            future_t = ambient_temp_c + (current_temp_c - ambient_temp_c) * decay
            return future_t

    @classmethod
    def evaluate_thermal_risk_margin(
        cls,
        current_temp_c: float,
        ambient_temp_c: float,
        compressor_status: CompressorStatus,
        cargo_profile: CargoProfile,
        projected_eta_minutes: float,
    ) -> Tuple[float, float, bool]:
        """
        Evaluates safety buffer between Thermal Buffer Time and Projected ETA.

        Returns:
            Tuple: (thermal_buffer_minutes, safety_margin_minutes, is_spoilage_imminent)
        """
        tbt = cls.calculate_thermal_buffer_time(
            current_temp_c=current_temp_c,
            ambient_temp_c=ambient_temp_c,
            compressor_status=compressor_status,
            cargo_profile=cargo_profile,
        )

        if math.isinf(tbt):
            return 999.0, 999.0, False

        safety_margin = tbt - projected_eta_minutes
        is_spoilage_imminent = safety_margin < 0.0

        return tbt, safety_margin, is_spoilage_imminent
