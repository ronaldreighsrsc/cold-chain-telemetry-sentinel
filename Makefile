.PHONY: test simulate benchmark dashboard api docker-up docker-down

test:
	pytest tests/ -v

simulate:
	python -m cold_chain_telemetry_sentinel.services.pipeline_runner simulate --steps 20

benchmark:
	python scripts/benchmark_throughput.py

dataset:
	python scripts/generate_sample_dataset.py

api:
	uvicorn cold_chain_telemetry_sentinel.services.api:app --host 0.0.0.0 --port 8000 --reload

dashboard:
	streamlit run cold_chain_telemetry_sentinel/dashboard/app.py

docker-up:
	docker compose up -d

docker-down:
	docker compose down
