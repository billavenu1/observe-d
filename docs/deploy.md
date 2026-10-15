# Deploying observe-d on a real GPU cluster (M8 MI300X, M9 H100)

Everything is rendered from the same files the local stack uses, so what was tested locally is what runs.

## 0. Prerequisites

| Item | Notes |
|---|---|
| 1–2 CPU nodes labelled `observe-d/role=observability` | Kafka, ClickHouse, gateway collectors, Prometheus/Grafana (MVP.md §2.1 sizing) |
| GPU node(s) | 8×MI300X or 8×H100 |
| Operators | Strimzi (Kafka), Altinity clickhouse-operator, kube-prometheus-stack, AMD or NVIDIA GPU Operator |
| NVMe StorageClass | set `storageClassName` in `deploy/backend/clickhouse.yaml` |
| Secret | `kubectl -n observability create secret generic clickhouse-otel --from-literal=password=<pw>` |

## 1. Backend

```bash
kubectl create namespace observability
```

```bash
kubectl apply -f deploy/backend/kafka.yaml -f deploy/backend/clickhouse.yaml
```

Apply the schema (port-forward ClickHouse HTTP, then run migrations):

```bash
kubectl -n observability port-forward svc/clickhouse-observe-d 8123:8123
```

```bash
CLICKHOUSE_PASSWORD=<pw> uv run python -m tools.migrate
```

The cost dictionaries in `0005_cost.sql` use inline `USER 'otel' PASSWORD 'otel'`. Before applying in
production, change them to your password or to a ClickHouse named collection.

## 2. Render and apply collectors, rules, dashboards

```bash
uv run python -m tools.render --cluster-name mi300x-a --out deploy/rendered
```

```bash
helm upgrade --install kps prometheus-community/kube-prometheus-stack -n monitoring --create-namespace --version 92.4.0 -f deploy/rendered/helm/kube-prometheus-stack-values.yaml
```

```bash
kubectl apply --server-side -f deploy/rendered/
```

```bash
helm upgrade --install otel-gateway open-telemetry/opentelemetry-collector -n observability -f deploy/helm/otel-gateway-values.yaml
```

## 3. GPU exporters

- **AMD**: AMD GPU Operator with the device metrics exporter enabled, Kubernetes pod mapping on.
- **NVIDIA**: GPU Operator with dcgm-exporter; add `DCGM_FI_DEV_FB_TOTAL`, `DCGM_FI_PROF_*` and
  `DCGM_FI_DEV_CLOCKS_EVENT_REASONS` to the exporter's custom metrics CSV (normalize/gpu.yaml lists them).

The agent discovers both by their standard pod labels (`app` / `app.kubernetes.io/name` containing
`dcgm-exporter` or `metrics-exporter`) and scrapes ports whose name contains `metrics`. If your exporter uses
different labels, annotate its pods with `observe-d.io/scrape: gpu` and `observe-d.io/port: "<port>"`.
Prometheus needs a ServiceMonitor/PodMonitor for the exporter; the GPU operators ship one.

## 4. llm-d / vLLM

On the vLLM pods (llm-d model server template):

| Setting | Value |
|---|---|
| annotations | `observe-d.io/scrape: engine`, `observe-d.io/port: "8000"` |
| args | `--otlp-traces-endpoint=http://otel-agent.observability.svc:4317` (and keep request logging off) |
| env | `OTEL_RESOURCE_ATTRIBUTES=llm.engine=vllm,llm.engine_version=<tag>`; `POD_NAME`/`NODE_NAME` via downward API |
| PodMonitor | port `8000` for Prometheus (llm:* recording rules) |

Gateway (Envoy): enable tracing with W3C propagation and an OpenTelemetry access logger to
`otel-agent.observability.svc:4317`, with the fields in [header-contract.md](header-contract.md) and
`log.source=gateway_access`. EPP: enable its tracing, and check which attributes it emits against
[telemetry-shapes.md](telemetry-shapes.md).

## 5. Validate (M8 checklist)

1. Capture real outputs into `sim/fixtures/<vendor>/`: GPU exporter `/metrics`, vLLM `/metrics`, a vLLM
   span, an EPP span and an access log, all as JSON from ClickHouse.
2. Diff them against `normalize/*.yaml` and [telemetry-shapes.md](telemetry-shapes.md). Fix the mappings or
   migrations, then run `uv run python -m normalize.generate` and re-apply.
3. Run `uv run pytest -m e2e` against the cluster (port-forward Prometheus to 9090 and ClickHouse to 8123).
4. Capacity test on the cluster's CPU nodes (results go into `docs/capacity.md`):

```bash
uv run python -m tools.loadtest --rate 5000 --duration 3600 --burst-rate 7000 --burst-at 1800 --burst-duration 900
```
