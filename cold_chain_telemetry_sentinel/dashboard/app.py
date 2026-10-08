"""
Streamlit Operational Dashboard for Cold-Chain Telemetry Sentinel.
Real-Time 3D Geospatial H3 Fleet Map, Thermal Buffer Monitoring, and Incident CEP Alerts.
"""

from __future__ import annotations

import json
from datetime import datetime
import pandas as pd
import pydeck as pdk
import streamlit as st

from cold_chain_telemetry_sentinel.domain.models import AlertLevel, CompressorStatus
from cold_chain_telemetry_sentinel.engines.stream_processor import StreamProcessor
from cold_chain_telemetry_sentinel.services.simulator import FleetSimulator

st.set_page_config(
    page_title="Cold-Chain Telemetry Sentinel",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Dark Theme Styling
st.markdown("""
<style>
    .metric-card {
        background: #111827;
        border: 1px solid #1F2937;
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .metric-title {
        color: #9CA3AF;
        font-size: 0.82rem;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
    .metric-value {
        color: #F9FAFB;
        font-size: 1.85rem;
        font-weight: 700;
        margin-top: 4px;
    }
    .metric-subtitle {
        color: #10B981;
        font-size: 0.8rem;
        margin-top: 2px;
    }
    .badge-emergency {
        background-color: #991B1B;
        color: #FEE2E2;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.75rem;
    }
    .badge-critical {
        background-color: #9A3412;
        color: #FFEDD5;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.75rem;
    }
    .badge-warning {
        background-color: #854D0E;
        color: #FEF9C3;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.75rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_sentinel_pipeline():
    processor = StreamProcessor()
    simulator = FleetSimulator()
    # Ingest initial incidents
    for inc in simulator.generate_incident_shocks():
        processor.broker.publish(StreamProcessor.TOPIC_TRAFFIC_INCIDENTS, inc)
    return processor, simulator


processor, simulator = get_sentinel_pipeline()

# Sidebar Controls
st.sidebar.title("❄️ Sentinel Control")
st.sidebar.markdown("**Event-Driven Cold-Chain Telemetry Engine**")

sim_steps = st.sidebar.slider("Simulation Steps", min_value=5, max_value=35, value=15, step=5)
inject_failure = st.sidebar.checkbox("Simulate TRU Compressor Failure", value=True)
failure_step = st.sidebar.slider("Failure Step Trigger", min_value=3, max_value=sim_steps, value=7)

if st.sidebar.button("▶ Run Live Stream Ingestion", type="primary"):
    with st.spinner("Processing real-time telemetry stream through CEP engine..."):
        stream_gen = simulator.generate_trip_telemetry_stream(
            steps_per_route=sim_steps,
            inject_failure_on_step=failure_step if inject_failure else 999,
            failure_vehicle_id="TRUCK-PHARMA-02",
        )
        for event in stream_gen:
            processor.handle_telemetry_event(event)
    st.sidebar.success(f"Stream ingestion complete! ({processor.metrics['consumed_events']} total events)")

if st.sidebar.button("🔄 Reset Fleet State"):
    st.cache_resource.clear()
    st.rerun()

# Top KPI Header
st.title("Cold-Chain Telemetry Sentinel")
st.caption("Real-Time Geospatial IoT Telemetry & Thermal Spoilage Disruption Engine | Santiago Logistics Grid")

m1, m2, m3, m4, m5 = st.columns(5)
fleet_summary = processor.analytics_sink.get_fleet_summary()

with m1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Monitored Fleet</div>
        <div class="metric-value">{fleet_summary.get('active_vehicles', 4)} Trucks</div>
        <div class="metric-subtitle">Active GPS & IoT probes</div>
    </div>
    """, unsafe_allow_html=True)

with m2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Processed Events</div>
        <div class="metric-value">{processor.metrics['processed_valid_events']}</div>
        <div class="metric-subtitle">DuckDB Columnar Store</div>
    </div>
    """, unsafe_allow_html=True)

with m3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Engine Latency</div>
        <div class="metric-value">{processor.metrics['avg_latency_ms']:.1f} ms</div>
        <div class="metric-subtitle">Sub-millisecond CEP loop</div>
    </div>
    """, unsafe_allow_html=True)

with m4:
    alert_count = processor.metrics['alerts_dispatched']
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Spoilage Alerts</div>
        <div class="metric-value" style="color: {'#EF4444' if alert_count > 0 else '#10B981'};">{alert_count}</div>
        <div class="metric-subtitle">Dispatched via Webhooks</div>
    </div>
    """, unsafe_allow_html=True)

with m5:
    dlq_count = processor.metrics['dlq_rejected_events']
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">DLQ Quarantined</div>
        <div class="metric-value">{dlq_count}</div>
        <div class="metric-subtitle">Data Quality Fault Gate</div>
    </div>
    """, unsafe_allow_html=True)

# Main Grid: Left = Map & Thermal; Right = Alerts & DLQ
col_left, col_right = st.columns([1.8, 1.2])

vehicles_data = processor.state_store.get_all_vehicles()
incidents_data = processor.cep_evaluator.get_active_incidents()
alerts_data = processor.cep_evaluator.get_alert_history()

with col_left:
    st.subheader("🗺️ Real-Time 3D Geospatial Fleet & H3 Disruption Map")

    # Prepare PyDeck Layers
    map_layers = []

    # Layer 1: Vehicles Current Position
    if vehicles_data:
        df_vehicles = pd.DataFrame(vehicles_data)
        
        # Color coding: Green = Normal, Orange = Degraded, Red = Failure
        def get_color(comp_status):
            if comp_status == CompressorStatus.FAILURE.value:
                return [239, 68, 68, 220]  # Red
            elif comp_status == CompressorStatus.DEGRADED.value:
                return [245, 158, 11, 220]  # Amber
            return [16, 185, 129, 220]      # Emerald Green

        df_vehicles["color"] = df_vehicles["compressor_status"].apply(get_color)

        vehicle_layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_vehicles,
            get_position=["longitude", "latitude"],
            get_color="color",
            get_radius=450,
            pickable=True,
            auto_highlight=True,
        )
        map_layers.append(vehicle_layer)

    # Layer 2: Traffic Incidents
    if incidents_data:
        df_incidents = pd.DataFrame([inc.to_dict() for inc in incidents_data])
        incident_layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_incidents,
            get_position=["longitude", "latitude"],
            get_color=[220, 38, 38, 180],
            get_radius=900,
            pickable=True,
        )
        map_layers.append(incident_layer)

    # Santiago View State
    view_state = pdk.ViewState(
        latitude=-33.4372,
        longitude=-70.6506,
        zoom=11.2,
        pitch=45,
        bearing=15,
    )

    r = pdk.Deck(
        layers=map_layers,
        initial_view_state=view_state,
        tooltip={"text": "Asset: {vehicle_id}\nTemp: {cargo_temp_c}°C\nSpeed: {speed_kmh} km/h\nTRU: {compressor_status}\nH3: {h3_index}"},
        map_style="mapbox://styles/mapbox/dark-v10",
    )
    st.pydeck_chart(r)

    # Temperature Evolution Chart
    st.subheader("📈 Cargo Thermal Telemetry & Degradation Dynamics")
    if vehicles_data:
        temp_df = pd.DataFrame([
            {
                "Vehicle": v["vehicle_id"],
                "Cargo Temp (°C)": v["cargo_temp_c"],
                "Ambient Temp (°C)": v["ambient_temp_c"],
                "Compressor": v["compressor_status"],
            }
            for v in vehicles_data
        ])
        st.dataframe(temp_df, use_container_width=True)

with col_right:
    st.subheader("🚨 Dispatched Operational Alerts (CEP Stream)")

    if alerts_data:
        for alert in reversed(alerts_data[-6:]):
            level = alert.alert_level.value
            badge_class = "badge-emergency" if "EMERGENCY" in level else ("badge-critical" if "CRITICAL" in level else "badge-warning")
            st.markdown(f"""
            <div style="background: #1F2937; border-left: 4px solid {'#EF4444' if 'EMERGENCY' in level else '#F59E0B'}; padding: 12px; border-radius: 6px; margin-bottom: 10px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span class="{badge_class}">{level}</span>
                    <span style="color: #9CA3AF; font-size: 0.75rem;">{alert.timestamp.strftime('%H:%M:%S UTC')}</span>
                </div>
                <div style="font-weight: 700; color: #F3F4F6; margin-top: 6px;">{alert.vehicle_id}</div>
                <div style="color: #D1D5DB; font-size: 0.85rem; margin-top: 4px;">{alert.root_cause}</div>
                <div style="color: #60A5FA; font-size: 0.8rem; margin-top: 6px; font-weight: 600;">↳ {alert.recommended_action}</div>
                <div style="color: #9CA3AF; font-size: 0.75rem; margin-top: 4px;">Thermal Buffer: <b>{alert.thermal_buffer_minutes:.0f}m</b> | ETA: <b>{alert.projected_eta_minutes:.0f}m</b> | Value at Risk: <b>${alert.commercial_risk_usd:,.0f} USD</b></div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No critical disruptions detected. Fleet operating within safe thermal buffers.")

    st.subheader("🛡️ Data Quality & Dead-Letter Queue (DLQ)")
    dlq_stats = processor.dlq_manager.get_stats()
    st.json(dlq_stats)

st.markdown("---")
st.caption("Engineered by Ronald Solares Chuquera | Industrial Civil Engineering & Real-Time Data Engineering")
