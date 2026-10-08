"""
Pipeline Runner CLI service.
Provides command-line orchestration for real-time simulation, batch runs,
DuckDB Parquet exports, and live metrics visualization.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cold_chain_telemetry_sentinel.engines.stream_processor import StreamProcessor
from cold_chain_telemetry_sentinel.services.simulator import FleetSimulator

app = typer.Typer(
    name="sentinel",
    help="Cold-Chain Telemetry Sentinel: Real-Time Geospatial IoT Disruption Engine.",
    add_completion=False,
)
console = Console()


@app.command()
def simulate(
    steps: int = typer.Option(20, "--steps", "-s", help="Number of telemetry steps per route"),
    fail_step: int = typer.Option(10, "--fail-step", "-f", help="Step index to inject TRU failure"),
    delay_ms: int = typer.Option(50, "--delay-ms", "-d", help="Artificial delay per event batch (ms)"),
):
    """
    Executes a high-fidelity streaming simulation of a cold-chain fleet with dynamic traffic shocks.
    """
    console.print(Panel.fit(
        "[bold cyan]Cold-Chain Telemetry Sentinel[/bold cyan]\n"
        "[dim]Real-Time Geospatial IoT Telemetry & Spoilage Prevention Engine[/dim]",
        border_style="cyan"
    ))

    processor = StreamProcessor()
    simulator = FleetSimulator()

    # 1. Ingest Traffic Incidents into the Stream Broker
    console.print("[yellow]Ingesting real-time traffic disruptions onto broker topic...[/yellow]")
    incidents = simulator.generate_incident_shocks()
    for inc in incidents:
        processor.broker.publish(StreamProcessor.TOPIC_TRAFFIC_INCIDENTS, inc)
        console.print(f" [red][DISRUPTION][/red] {inc['description']} ([bold]{inc['h3_index']}[/bold])")

    console.print(f"\n[green]Streaming fleet telemetry ({steps} steps across active corridors)...[/green]\n")

    alert_count = 0
    event_gen = simulator.generate_trip_telemetry_stream(
        steps_per_route=steps,
        inject_failure_on_step=fail_step,
        failure_vehicle_id="TRUCK-PHARMA-02",
    )

    for event in event_gen:
        alert = processor.handle_telemetry_event(event)
        if alert:
            alert_count += 1
            color = "red" if "EMERGENCY" in alert.alert_level.value else ("yellow" if "CRITICAL" in alert.alert_level.value else "blue")
            console.print(
                f"[{color}][ALERT - {alert.alert_level.value}][/{color}] "
                f"[bold]{alert.vehicle_id}[/bold] | Temp: {alert.current_temp_c:.1f}C (Crit: {alert.critical_threshold_c}C) | "
                f"Buffer: {alert.thermal_buffer_minutes:.0f}m | ETA: {alert.projected_eta_minutes:.0f}m\n"
                f"  -> [dim]{alert.recommended_action}[/dim]"
            )
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    # Display Final Operational Metrics
    console.print("\n" + "=" * 60)
    console.print("[bold green]Streaming Run Complete! Summary Metrics:[/bold green]\n")

    table = Table(title="Sentinel Stream Processor KPIs", border_style="green")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    for k, v in processor.metrics.items():
        table.add_row(k.replace("_", " ").title(), str(v))

    console.print(table)

    summary = processor.analytics_sink.get_fleet_summary()
    console.print(f"\n[cyan]DuckDB OLAP Aggregates:[/cyan] {summary}")


@app.command()
def export(
    output_path: str = typer.Option("data/telemetry_export.parquet", "--output", "-o", help="Parquet destination path"),
):
    """
    Exports embedded DuckDB telemetry history to Parquet format.
    """
    processor = StreamProcessor()
    # Populate a run first if empty
    sim = FleetSimulator()
    for ev in sim.generate_trip_telemetry_stream(steps_per_route=5):
        processor.handle_telemetry_event(ev)

    success = processor.analytics_sink.export_to_parquet(output_path)
    if success:
        console.print(f"[green]Successfully exported telemetry events to Parquet at:[/green] {output_path}")
    else:
        console.print("[red]Parquet export failed.[/red]")


def main():
    app()


if __name__ == "__main__":
    main()
