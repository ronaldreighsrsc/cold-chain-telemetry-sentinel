param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("test", "simulate", "benchmark", "dataset", "api", "dashboard", "docker-up", "docker-down")]
    [string]$Task = "test"
)

$ErrorActionPreference = "Stop"

switch ($Task) {
    "test" {
        py -m pytest tests/ -v
    }
    "simulate" {
        py -m cold_chain_telemetry_sentinel.services.pipeline_runner simulate --steps 20
    }
    "benchmark" {
        py scripts/benchmark_throughput.py
    }
    "dataset" {
        py scripts/generate_sample_dataset.py
    }
    "api" {
        py -m uvicorn cold_chain_telemetry_sentinel.services.api:app --host 0.0.0.0 --port 8000 --reload
    }
    "dashboard" {
        py -m streamlit run cold_chain_telemetry_sentinel/dashboard/app.py
    }
    "docker-up" {
        docker compose up -d
    }
    "docker-down" {
        docker compose down
    }
}
