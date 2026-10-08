"""
Analytical OLAP sink powered by embedded DuckDB and columnar Parquet.
Stores long-term fleet trajectories, environmental histories, and incident metrics.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


class AnalyticsSink:
    """
    Embedded columnar OLAP database for real-time telemetry analytics.
    Enables instant SQL aggregations over millions of streaming events.
    """

    def __init__(self, db_path: Optional[str] = ":memory:"):
        self.db_path = db_path
        self._conn = None
        self._in_memory_records: List[Dict[str, Any]] = []

        if HAS_DUCKDB:
            try:
                self._conn = duckdb.connect(db_path)
                self._init_tables()
            except Exception as e:
                logger.warning(f"DuckDB initialization failed ({e}), using in-memory list sink.")
                self._conn = None

    def _init_tables(self) -> None:
        if not self._conn:
            return
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_events (
                vehicle_id VARCHAR,
                timestamp TIMESTAMP,
                latitude DOUBLE,
                longitude DOUBLE,
                speed_kmh DOUBLE,
                ambient_temp_c DOUBLE,
                cargo_temp_c DOUBLE,
                compressor_status VARCHAR,
                battery_soc_pct DOUBLE,
                h3_index VARCHAR
            );
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS operational_alerts (
                alert_id VARCHAR,
                vehicle_id VARCHAR,
                timestamp TIMESTAMP,
                alert_level VARCHAR,
                current_temp_c DOUBLE,
                critical_threshold_c DOUBLE,
                thermal_buffer_minutes DOUBLE,
                projected_eta_minutes DOUBLE,
                root_cause VARCHAR,
                recommended_action VARCHAR,
                commercial_risk_usd DOUBLE
            );
        """)

    def insert_telemetry_batch(self, records: List[Dict[str, Any]]) -> None:
        """Appends a batch of validated telemetry events."""
        if not records:
            return

        if self._conn:
            try:
                values = [
                    (
                        r["vehicle_id"],
                        r["timestamp"],
                        r["latitude"],
                        r["longitude"],
                        r["speed_kmh"],
                        r["ambient_temp_c"],
                        r["cargo_temp_c"],
                        r["compressor_status"],
                        r["battery_soc_pct"],
                        r["h3_index"],
                    )
                    for r in records
                ]
                self._conn.executemany("""
                    INSERT INTO telemetry_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, values)
                return
            except Exception as e:
                logger.error(f"Error inserting batch into DuckDB: {e}")

        self._in_memory_records.extend(records)

    def insert_alert(self, alert_dict: Dict[str, Any]) -> None:
        """Persists an operational alert event."""
        if self._conn:
            try:
                self._conn.execute("""
                    INSERT INTO operational_alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    alert_dict["alert_id"],
                    alert_dict["vehicle_id"],
                    alert_dict["timestamp"],
                    alert_dict["alert_level"],
                    alert_dict["current_temp_c"],
                    alert_dict["critical_threshold_c"],
                    alert_dict["thermal_buffer_minutes"],
                    alert_dict["projected_eta_minutes"],
                    alert_dict["root_cause"],
                    alert_dict["recommended_action"],
                    alert_dict.get("commercial_risk_usd", 0.0),
                ))
            except Exception as e:
                logger.error(f"Error inserting alert into DuckDB: {e}")

    def get_fleet_summary(self) -> Dict[str, Any]:
        """Calculates macro operational KPIs across stored events."""
        if self._conn:
            try:
                res = self._conn.execute("""
                    SELECT 
                        COUNT(*) as total_events,
                        COUNT(DISTINCT vehicle_id) as active_vehicles,
                        AVG(cargo_temp_c) as avg_cargo_temp,
                        AVG(speed_kmh) as avg_speed,
                        MIN(cargo_temp_c) as min_cargo_temp,
                        MAX(cargo_temp_c) as max_cargo_temp
                    FROM telemetry_events
                """).fetchone()
                if res and res[0] > 0:
                    return {
                        "total_events": res[0],
                        "active_vehicles": res[1],
                        "avg_cargo_temp_c": round(res[2] or 0.0, 2),
                        "avg_speed_kmh": round(res[3] or 0.0, 1),
                        "min_cargo_temp_c": round(res[4] or 0.0, 2),
                        "max_cargo_temp_c": round(res[5] or 0.0, 2),
                    }
            except Exception:
                pass

        # Fallback summary calculation
        count = len(self._in_memory_records)
        return {
            "total_events": count,
            "active_vehicles": len(set(r.get("vehicle_id") for r in self._in_memory_records)),
            "avg_cargo_temp_c": round(sum(r.get("cargo_temp_c", 0) for r in self._in_memory_records) / max(1, count), 2),
            "avg_speed_kmh": round(sum(r.get("speed_kmh", 0) for r in self._in_memory_records) / max(1, count), 1),
            "min_cargo_temp_c": min((r.get("cargo_temp_c", 0) for r in self._in_memory_records), default=0.0),
            "max_cargo_temp_c": max((r.get("cargo_temp_c", 0) for r in self._in_memory_records), default=0.0),
        }

    def export_to_parquet(self, output_path: str) -> bool:
        """Exports the event history to an optimized Parquet columnar file."""
        if not self._conn:
            return False
        try:
            p = Path(output_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            self._conn.execute(f"COPY telemetry_events TO '{output_path}' (FORMAT PARQUET);")
            return True
        except Exception as e:
            logger.error(f"Failed exporting to Parquet: {e}")
            return False
