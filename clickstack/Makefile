include .env

run_hyperdx:
	podman run --name hyperdx -p 8080:8080 -p 4317:4317 -p 4318:4318 hyperdx/hyperdx-all-in-one:latest

load_sample:
	./load-data.sh

client_demo:
# 	python3 -m venv hyperdx
# 	source hyperdx/bin/activate
# 	pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp
# 	python custom_metrics.py
