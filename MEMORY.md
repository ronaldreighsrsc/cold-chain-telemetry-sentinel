# 🧠 MEMORY.md — Persistent Repository Memory for cold-chain-telemetry-sentinel

## 📍 1. Estado Actual del Proyecto
- **Fase:** Producción v1.0 — Spec 001 Completada (`26 passed` en `pytest`, 100% en verde).
- **Repositorio GitHub:** `https://github.com/ronaldreighsrsc/cold-chain-telemetry-sentinel`
- **Arquitectura Central (`cold_chain_telemetry_sentinel/`):**
  1. **Motor Termodinámico USP <1079> (`mkt_engine.py`):** Cálculo exacto de *Mean Kinetic Temperature (MKT)* mediante ecuación de Arrhenius ($E_a = 83.144\text{ kJ/mol}$) y evaluación de excursiones térmicas FDA 21 CFR Part 11 / EU GMP Annex 11.
  2. **Detección Estadística de Anomalías (`anomaly_detector.py`):** Control Estadístico de Procesos (SPC) con cartas **CUSUM tabular** y **EWMA** para detectar deriva gradual de compresores antes de romper el umbral de $8.0\,^\circ\text{C}$.
  3. **Data Contract Gate & Quarantine DLQ (`contract_gate.py`):** Enrutamiento determinista de tramas IoT corruptas (`NaN`, saltos imposibles, relojes invertidos) hacia cola de cuarentena sin detener el pipeline.
  4. **Almacenamiento Dual (`storage.py`):** `SQLite WAL` para trazabilidad transaccional ACID y `DuckDB` para analítica OLAP vectorizada sobre lotes farmacéuticos.

## ⚖️ 2. Decisiones Arquitectónicas Activas
- **MKT Arrhenius sobre Promedio Aritmético:** El promedio simple subestima la degradación química exponencial durante picos de calor; la fórmula de Haynes pondera cada lectura por su impacto cinético real.
- **CUSUM + EWMA sobre Umbrales Estáticos:** Permite mantenimiento predictivo al capturar derivas térmicas de $+0.15\,^\circ\text{C/h}$ mientras la cámara aún marca $< 8.0\,^\circ\text{C}$.

## ⚠️ 3. Reglas No Obvias y Guardrails
- Las temperaturas deben convertirse siempre a Kelvin ($T_K = T_C + 273.15$) antes de evaluar $\exp(-\Delta H / (R \cdot T_K))$ para evitar desbordamientos numéricos.
