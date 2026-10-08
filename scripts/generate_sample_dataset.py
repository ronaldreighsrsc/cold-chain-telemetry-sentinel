"""
Sample Dataset Generator for Cold-Chain Telemetry Sentinel.
Exports benchmark Parquet and GeoJSON files for offline exploratory data analysis.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from rich.console import Console

from cold_chain_telemetry_sentinel.connectors.analytics_sink import AnalyticsSink
from cold_chain_telemetry_sentinel.engines.stream_processor import StreamProcessor
from cold_chain_telemetry_sentinel.services.simulator import FleetSimulator

console = Console()


def generate_offline_dataset(steps: int = 50) -> None:
    console.print("[bold cyan]Generating sample Parquet and GeoJSON datasets...[/bold cyan]")

    sink = AnalyticsSink(":memory:")
    processor = StreamProcessor(analytics_sink=sink)
    simulator = FleetSimulator()

    incidents = simulator.generate_incident_shocks()
    for inc in incidents:
        processor.broker.publish(StreamProcessor.TOPIC_TRAFFIC_INCIDENTS, inc)

    events_list = []
    for ev in simulator.generate_trip_telemetry_stream(steps_per_route=steps, inject_failure_on_step=20):
        processor.handle_telemetry_event(ev)
        events_list.append(ev)

    data_dir = ROOT / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Export to Parquet
    parquet_path = data_dir / "sample_coldchain_telemetry.parquet"
    df = pd.DataFrame(events_list)
    df.to_parquet(parquet_path, index=False)
    console.print(f"[green][OK] Parquet dataset written ({len(df):,} records):[/green] {parquet_path}")

    # 2. Export Incidents to GeoJSON
    geojson_path = data_dir / "sample_incidents.geojson"
    features = []
    for inc in incidents:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [inc["longitude"], inc["latitude"]],
            },
            "properties": {
                "incident_id": inc["incident_id"],
                "description": inc["description"],
                "severity": inc["severity"],
                "delay_minutes": inc["delay_impact_minutes"],
                "h3_index": inc["h3_index"],
            },
        }
        features.append(feature)

    geojson_obj = {
        "type": "FeatureCollection",
        "features": features,
    }
    geojson_path.write_text(json.dumps(geojson_obj, indent=2), encoding="utf-8")
    console.print(f"[green][OK] GeoJSON incidents written:[/green] {geojson_path}")


if __name__ == "__main__":
    generate_offline_dataset(50)
