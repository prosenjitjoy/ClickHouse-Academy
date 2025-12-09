from opentelemetry import metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes as RA
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from datetime import datetime
import atexit, time, random

# config
COLLECTOR_METRICS_ENDPOINT = "http://localhost:4318/v1/metrics"
HEADERS = {"authorization": "f490d497-b479-4a85-aecc-473f4dab5689"}
EXPORT_INTERVAL_SEC = 5
SERVICE_NAME="custom-metrics-service"
METRIC_NAME="requests_processed"
METRIC_DESC="Number of processed requests"

# setup
resource = Resource.create({RA.SERVICE_NAME: SERVICE_NAME})
exporter = OTLPMetricExporter(endpoint=COLLECTOR_METRICS_ENDPOINT, headers=HEADERS)
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=EXPORT_INTERVAL_SEC * 1000)

provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(provider)
atexit.register(provider.shutdown)  # flush on exit

# metric
meter = metrics.get_meter("custom-metrics-meter")
metric_counter = meter.create_counter(METRIC_NAME, unit="1", description=METRIC_DESC)

# --- data generation loop ---
try:
    ENV = ["dev", "stage", "prod"]
    while True:
        now = datetime.now()
        value = random.randint(1, 100)
        env = ENV[random.randint(0, 2)]
        metric_counter.add(value, {"env": env})
        print(f"[{now.strftime('%H:%M:%S')}] Added: value={value} env={env}")
        time.sleep(1)
except KeyboardInterrupt:
    pass  # atexit flushes
