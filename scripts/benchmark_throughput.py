"""
High-Throughput Performance and Latency Benchmark Script.
Stress-tests the Sentinel stream engine with 10,000+ synthetic events,
calculating ingestion throughput and P50/P95/P99 latency profiles.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
from rich.console import Console
from rich.table import Table

from cold_chain_telemetry_sentinel.engines.stream_processor import StreamProcessor
from cold_chain_telemetry_sentinel.services.simulator import FleetSimulator

console = Console()


def run_benchmark(event_count: int = 5000) -> None:
    console.print(f"[bold cyan]Running Sentinel Ingestion Stress Benchmark ({event_count:,} events)...[/bold cyan]\n")

    processor = StreamProcessor()
    simulator = FleetSimulator()

    # Ingest disruption base
    for inc in simulator.generate_incident_shocks():
        processor.broker.publish(StreamProcessor.TOPIC_TRAFFIC_INCIDENTS, inc)

    # Pre-generate event payloads to isolate pipeline processing time
    console.print("[dim]Generating synthetic event batch in memory...[/dim]")
    events: List[dict] = []
    stream_gen = simulator.generate_trip_telemetry_stream(steps_per_route=event_count // 4 + 10)
    for _ in range(event_count):
        events.append(next(stream_gen))

    console.print("[green]Batch prepared. Executing streaming pipeline...[/green]")

    latencies_ms: List[float] = []
    start_total = time.perf_counter()

    for ev in events:
        t0 = time.perf_counter()
        processor.handle_telemetry_event(ev)
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)

    total_time_sec = time.perf_counter() - start_total
    throughput_eps = len(events) / total_time_sec

    # Percentiles
    p50 = float(np.percentile(latencies_ms, 50))
    p90 = float(np.percentile(latencies_ms, 90))
    p95 = float(np.percentile(latencies_ms, 95))
    p99 = float(np.percentile(latencies_ms, 99))
    max_lat = float(np.max(latencies_ms))

    table = Table(title="Sentinel Stream Processing Benchmark Results", border_style="cyan")
    table.add_column("Benchmark Metric", style="yellow")
    table.add_column("Measurement", style="bold green")

    table.add_row("Total Events Processed", f"{len(events):,}")
    table.add_row("Wall-Clock Time", f"{total_time_sec:.3f} s")
    table.add_row("Throughput", f"{throughput_eps:,.1f} events/sec")
    table.add_row("P50 Latency (Median)", f"{p50:.3f} ms")
    table.add_row("P90 Latency", f"{p90:.3f} ms")
    table.add_row("P95 Latency", f"{p95:.3f} ms")
    table.add_row("P99 Latency", f"{p99:.3f} ms")
    table.add_row("Max Observed Latency", f"{max_lat:.3f} ms")

    console.print(table)

    results = {
        "event_count": len(events),
        "total_time_seconds": round(total_time_sec, 3),
        "throughput_events_per_second": round(throughput_eps, 1),
        "p50_latency_ms": round(p50, 3),
        "p95_latency_ms": round(p95, 3),
        "p99_latency_ms": round(p99, 3),
    }

    out_file = Path("data/benchmark_results.json")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    console.print(f"\n[dim]Benchmark results exported to {out_file}[/dim]")


if __name__ == "__main__":
    run_benchmark(5000)
